package io.digdag.standards.operator.redshift;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.*;
import io.digdag.standards.operator.jdbc.*;
import io.digdag.util.DurationParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RedshiftLoadOperatorFactory
        implements OperatorFactory
{
    private static final String OPERATOR_TYPE = "redshift_load";
    private final TemplateEngine templateEngine;

    private static final String QUERY_ID = "queryId";

    @Inject
    public RedshiftLoadOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return OPERATOR_TYPE;
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new RedshiftLoadOperator(projectPath, request, templateEngine);
    }

    private static class RedshiftLoadOperator
        extends AbstractJdbcOperator<RedshiftConnectionConfig>
    {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        private RedshiftLoadOperator(Path projectPath, TaskRequest request, TemplateEngine templateEngine)
        {
            super(projectPath, request, templateEngine);
        }

        /* TODO: This method name should be connectionConfig() or something? */
        @Override
        protected RedshiftConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return RedshiftConnectionConfig.configure(secrets, params);
        }

        /* TODO: This method should be in XxxxConnectionConfig ? */
        @Override
        protected RedshiftConnection connect(RedshiftConnectionConfig connectionConfig)
        {
            return RedshiftConnection.open(connectionConfig);
        }

        @Override
        protected String type()
        {
            return OPERATOR_TYPE;
        }

        @Override
        protected TaskResult run(TaskExecutionContext ctx, Config params, Config state, RedshiftConnectionConfig connectionConfig)
        {
            /*
            redshift_load>: dest_table
            source: s3://mybucket/data/listing/     // i.g. 'emr://j-SAMPLE2B500FC/myoutput/part-*'
            format: JSON            // Optional (AVRO/CSV/DELIMITER/...)
            quote_char: "           // Optional (For CSV)
            delimiter_char: '|'     // Optional (For DELIMITER)
            fixedwidth_spec: 'colLabel1:colWidth1,colLabel:colWidth2, ...'      // Optional (For FIXEDWIDTH)
            jsonpaths_file: s3://mybucket/jsonpaths.txt'    // Optional (For AVRO or JSON)
            compression: LZOP       // Optional (BZIP2/GZIP/LZOP)
            readratio: 50           // Optional
            manifest: true          // Optional
            removequotes: true      // Optional
            emptyasnull: true       // Optional
            blanksasnull: true      // Optional
            maxerror: 5             // Optional
            timeformat: 'YYYY-MM-DD HH:MI:SS'   // Optional
            explicit_ids: true      // Optional
            */
            String destTable = params.get("_command", String.class);
            String sourceUri = params.get("source", String.class);

            String format = params.get("format", String.class);
            Optional<Boolean> manifest = params.getOptional("manifest", Boolean.class);
            Optional<Character> quoteChar = params.getOptional("quote_char", Character.class);
            Optional<Character> delimiterChar = params.getOptional("delimiter_char", Character.class);
            Optional<String> jsonpathsFile = params.getOptional("jsonpaths_file", String.class);
            Optional<String> compression = params.getOptional("compression", String.class);
            Optional<Integer> readratio = params.getOptional("read_ratio", Integer.class);
            Optional<Boolean> removeQuotes = params.getOptional("remove_quotes", Boolean.class);
            Optional<Boolean> emptyAsNull = params.getOptional("empty_as_null", Boolean.class);
            Optional<Boolean> blankAsNull = params.getOptional("blank_as_null", Boolean.class);
            Optional<Integer> maxError = params.getOptional("max_error", Integer.class);
            Optional<String> timeFormat = params.getOptional("time_format", String.class);
            Optional<Boolean> explicitIds = params.getOptional("explicit_ids", Boolean.class);

            SecretProvider awsSecrets = ctx.secrets().getSecrets("aws");
            SecretProvider s3Secrets = awsSecrets.getSecrets("s3");

            String accessKey = s3Secrets.getSecretOptional("access-key-id")
                    .or(() -> awsSecrets.getSecret("access-key-id"));

            String secretKey = s3Secrets.getSecretOptional("secret-access-key")
                    .or(() -> awsSecrets.getSecret("secret-access-key"));

            List<Object> paramsInSql = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            // main
            sb.append("COPY ? FROM '?'\n");
            paramsInSql.add(destTable);
            paramsInSql.add(sourceUri);
            // credentials
            // TODO: Support ENCRYPTED
            sb.append("CREDENTIALS 'aws_access_key_id=?;aws_secret_access_key=?'\n");
            paramsInSql.add(accessKey);
            paramsInSql.add(secretKey);
            // manifests
            if (manifest.or(false)) {
                sb.append("MANIFEST\n");
            }
            // format
            sb.append("?");
            paramsInSql.add(format);
            switch (format.toUpperCase()) {
                case "CSV":
                    if (quoteChar.isPresent()) {
                        sb.append(" QUOTE '?'");
                        paramsInSql.add(quoteChar.get());
                    }
                    break;
                case "DELIMITER":
                    if (delimiterChar.isPresent()) {
                        sb.append(" '?'");
                        paramsInSql.add(delimiterChar.get());
                    }
                    break;
                case "FIXEDWIDTH":
                    String fixedwidthSpec = params.get("fixedwidth_spec", String.class);
                    sb.append(" '?'");
                    paramsInSql.add(fixedwidthSpec);
                    break;
                case "AVRO":
                case "JSON":
                    if (jsonpathsFile.isPresent()) {
                        sb.append(" '?'");
                        paramsInSql.add(jsonpathsFile.get());
                    }
                    break;
            }
            sb.append("\n");

            // compression
            if (compression.isPresent()) {
                sb.append("?\n");
                paramsInSql.add(compression.get());
            }
            // readratio
            if (readratio.isPresent()) {
                sb.append("readratio ?\n");
                paramsInSql.add(readratio.get());
            }
            // removeQuotes
            if (removeQuotes.isPresent()) {
                sb.append("removequotes\n");
            }
            // emptyAsNull
            if (emptyAsNull.isPresent()) {
                sb.append("emptyasnull\n");
            }
            // blankAsNull
            if (blankAsNull.isPresent()) {
                sb.append("blankasnull\n");
            }
            // maxError
            if (maxError.isPresent()) {
                sb.append("maxerror ?\n");
                paramsInSql.add(maxError.get());
            }
            // timeFormat
            if (timeFormat.isPresent()) {
                sb.append("timeformat ?\n");
                paramsInSql.add(timeFormat.get());
            }
            // explicitIds
            if (explicitIds.isPresent()) {
                sb.append("explicit_ids\n");
            }

            String query = sb.toString();

        boolean strictTransaction = strictTransaction(params);

        String statusTableName;
        DurationParam statusTableCleanupDuration;
        if (strictTransaction) {
            statusTableName = params.get("status_table", String.class, "__digdag_status");
            statusTableCleanupDuration = params.get("status_table_cleanup", DurationParam.class,
                    DurationParam.of(Duration.ofHours(24)));
        }
        else {
            statusTableName = null;
            statusTableCleanupDuration = null;
        }

        UUID queryId;
            // generate query id
            if (!state.has(QUERY_ID)) {
                // this is the first execution of this task
                logger.debug("Generating query id for a new {} task", type());
                queryId = UUID.randomUUID();
                state.set(QUERY_ID, queryId);
                throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
            }
            queryId = state.get(QUERY_ID, UUID.class);

        try (JdbcConnection connection = connect(connectionConfig)) {
            Exception statementError = connection.validateStatement(query);
            if (statementError != null) {
                throw new ConfigException("Given query is invalid", statementError);
            }

                String statement;
                boolean statementMayReturnResults;
            statement = connection.buildInsertStatement(query, insertInto.get());

                    Exception modifiedStatementError = connection.validateStatement(statement);
                    if (modifiedStatementError != null) {
                        throw new ConfigException("Given query is valid but failed to build INSERT INTO statement (this may happen if given query includes multiple statements or semicolon \";\"?)", modifiedStatementError);
                    }
                    statementMayReturnResults = false;
                    logger.debug("Running a modified statement: {}", statement);

                TransactionHelper txHelper;
                if (strictTransaction) {
                    txHelper = connection.getStrictTransactionHelper(statusTableName,
                            statusTableCleanupDuration.getDuration());
                }
                else {
                    txHelper = new NoTransactionHelper();
                }

                txHelper.prepare();

                boolean executed = txHelper.lockedTransaction(queryId, () -> {
                    connection.executeUpdate(statement);
                });

                if (!executed) {
                    logger.debug("Query is already completed according to status table. Skipping statement execution.");
                }

                try {
                    txHelper.cleanup();
                }
                catch (Exception ex) {
                    logger.warn("Error during cleaning up status table. Ignoring.", ex);
                }

                return TaskResult.defaultBuilder(request).build();
        }
        catch (NotReadOnlyException ex) {
            throw new ConfigException("Query must be read-only if download_file is set", ex.getCause());
        }
        catch (LockConflictException ex) {
            int pollingInterval = state.get(POLL_INTERVAL, Integer.class, INITIAL_POLL_INTERVAL);
            // Set next interval for exponential backoff
            state.set(POLL_INTERVAL, Math.min(pollingInterval * 2, MAX_POLL_INTERVAL));
            throw TaskExecutionException.ofNextPolling(pollingInterval, ConfigElement.copyOf(state));
        }
        catch (DatabaseException ex) {
            // expected error that should suppress stacktrace by default
            String message = String.format("%s [%s]", ex.getMessage(), ex.getCause().getMessage());
            throw new TaskExecutionException(message, buildExceptionErrorConfig(ex));
        }
            return null;
        }
    }
}
