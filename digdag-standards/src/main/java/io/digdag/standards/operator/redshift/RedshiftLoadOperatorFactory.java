package io.digdag.standards.operator.redshift;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.*;
import io.digdag.standards.operator.jdbc.AbstractJdbcJobOperator;
import io.digdag.standards.operator.jdbc.AbstractJdbcOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class RedshiftLoadOperatorFactory
        implements OperatorFactory
{
    private static final String OPERATOR_TYPE = "redshift_load";
    private final TemplateEngine templateEngine;

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
        protected TaskResult run(TaskExecutionContext ctx, Config params, Config state, RedshiftConnectionConfig connectionConfig) {
            /* TODO: Not implemented yet */
            return null;
        }

        @Override
        protected boolean strictTransaction(Config params)
        {
            if (params.getOptional("strict_transaction", Boolean.class).isPresent()) {
                // RedShift doesn't support "SELECT FOR UPDATE" statement
                logger.warn("'strict_transaction' is ignored in 'redshift' operator");
            }
            return false;
        }
    }
}
