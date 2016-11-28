package acceptance.td;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import io.digdag.standards.operator.redshift.RedshiftConnection;
import io.digdag.standards.operator.redshift.RedshiftConnectionConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.*;

public class RedshiftIT
{
    private static final String REDSHIFT_CONFIG = System.getenv("REDSHIFT_IT_CONFIG");
    private static final String RESTRICTED_USER = "not_admin";
    private static final String SRC_TABLE = "src_tbl";
    private static final String DEST_TABLE = "dest_tbl";

    private static final Config EMPTY_CONFIG = configFactory().create();

    private TemporaryDigdagServer server;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private Path projectDir;
    private Config config;
    private String redshiftHost;
    private String redshiftDatabase;
    private String redshiftUser;
    private String redshiftPassword;
    private String database;
    private String restrictedUserPassword;
    private Path configFile;

    @Before
    public void setUp() throws Exception {
        assumeThat(REDSHIFT_CONFIG, not(isEmptyOrNullString()));

        ObjectMapper objectMapper = DigdagClient.objectMapper();
        config = Config.deserializeFromJackson(objectMapper, objectMapper.readTree(REDSHIFT_CONFIG));
        redshiftHost = config.get("host", String.class);
        redshiftDatabase = config.get("database", String.class);
        redshiftUser = config.get("user", String.class);
        redshiftPassword = config.get("password", String.class);

        database = "redshiftoptest_" + UUID.randomUUID().toString().replace('-', '_');
        restrictedUserPassword = UUID.randomUUID() + "0aZ";

        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        configFile = folder.newFile().toPath();
        Files.write(configFile, asList("secrets.redshift.password= " + redshiftPassword));

        createTempDatabase();

        setupRestrictedUser();

        setupSourceTable();
    }

    @After
    public void tearDown()
    {
        if (config != null) {
            try {
                if (database != null) {
                    removeTempDatabase();
                }
            }
            finally {
                removeRestrictedUser();
            }
        }

        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Test
    public void selectAndDownload()
            throws Exception
    {
        copyResource("acceptance/redshift/select_download.dig", projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));
        Path resultFile = folder.newFolder().toPath().resolve("result.csv");
        TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-p", "result_file=" + resultFile.toString(),
                "-c", configFile.toString(),
                "redshift.dig");

        assertThat(Files.exists(resultFile), is(true));

        List<String> csvLines = Files.readAllLines(resultFile);
        assertThat(csvLines.toString(), is(stringContainsInOrder(
                asList("id,name,score", "0,foo,3.14", "1,bar,1.23", "2,baz,5.0")
        )));
    }

    @Test
    public void createTable()
            throws Exception
    {
        copyResource("acceptance/redshift/create_table.dig", projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));

        setupDestTable();

        TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-c", configFile.toString(),
                "redshift.dig");

        assertTableContents(DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f)
        ));
    }

    @Test
    public void insertInto()
            throws Exception
    {
        copyResource("acceptance/redshift/insert_into.dig", projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));

        setupDestTable();

        TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-c", configFile.toString(),
                "redshift.dig");

        assertTableContents(DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
        ));
    }

    private void setupSourceTable()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE TABLE " + SRC_TABLE + " (id integer, name text, score real)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (0, 'foo', 3.14)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (1, 'bar', 1.23)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (2, 'baz', 5.00)");

            conn.executeUpdate("GRANT SELECT ON " + SRC_TABLE +  " TO " + RESTRICTED_USER);
        }
    }

    private void setupRestrictedUser()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            try {
                conn.executeUpdate("CREATE USER " + RESTRICTED_USER + " WITH PASSWORD '" + restrictedUserPassword + "'");
            }
            catch (DatabaseException e) {
                // 42710: duplicate_object
                if (!e.getCause().getSQLState().equals("42710")) {
                    throw e;
                }
            }
        }
    }

    private void setupDestTable()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE TABLE IF NOT EXISTS " + DEST_TABLE + " (id integer, name text, score real)");
            conn.executeUpdate("DELETE FROM " + DEST_TABLE + " WHERE id = 9");
            conn.executeUpdate("INSERT INTO " + DEST_TABLE + " (id, name, score) VALUES (9, 'zzz', 9.99)");

            conn.executeUpdate("GRANT INSERT ON " + DEST_TABLE +  " TO " + RESTRICTED_USER);
        }
    }

    private void assertTableContents(String table, List<Map<String, Object>> expected)
            throws NotReadOnlyException
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeReadOnlyQuery(String.format("SELECT * FROM %s ORDER BY id", table),
                    (rs) -> {
                        assertThat(rs.getColumnNames(), is(asList("id", "name", "score")));
                        int index = 0;
                        List<Object> row;
                        while ((row = rs.next()) != null) {
                            Map<String, Object> expectedRow = expected.get(index);

                            int id = (int) row.get(0);
                            assertThat(id, is(expectedRow.get("id")));

                            String name = (String) row.get(1);
                            assertThat(name, is(expectedRow.get("name")));

                            float score = (float) row.get(2);
                            assertThat(score, is(expectedRow.get("score")));

                            index++;
                        }
                        assertThat(index, is(expected.size()));
                    }
            );
        }
    }

    private SecretProvider getDatabaseSecrets()
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", redshiftHost,
                "user", redshiftUser,
                "password", redshiftPassword,
                "database", database
        ).get(key));
    }

    private SecretProvider getAdminDatabaseSecrets()
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", redshiftHost,
                "user", redshiftUser,
                "password", redshiftPassword,
                "database", redshiftDatabase
        ).get(key));
    }

    private void createTempDatabase()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE DATABASE " + database);
        }
    }

    private void removeTempDatabase()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            // Redshift doesn't support 'DROP DATABASE IF EXISTS'...
            conn.executeUpdate("DROP DATABASE " + database);
        }
    }
    private void removeRestrictedUser()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("DROP USER IF EXISTS " + RESTRICTED_USER);
        }
    }
}
