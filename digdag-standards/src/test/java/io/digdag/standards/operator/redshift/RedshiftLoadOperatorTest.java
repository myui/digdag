package io.digdag.standards.operator.redshift;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretProvider;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedshiftLoadOperatorTest
{
    @Test
    public void createCopyStatement()
            throws Exception
    {
        Config config = new ConfigFactory(DigdagClient.objectMapper())
                .create(ImmutableMap
                        .builder()
                        .put("_command", "test_table")
                        .put("source", "s3://mybucket/data/listing/")
                        .put("format", "json")
                        .build()
                );

        SecretProvider redshiftSecrets = mock(SecretProvider.class);
        when(redshiftSecrets.getSecretOptional("access-key-id"))
                .thenReturn(Optional.of("test-redshift-access-key-id"));
        when(redshiftSecrets.getSecretOptional("secret-access-key"))
                .thenReturn(Optional.of("test-redshift-secret-access-key"));

        SecretProvider redshiftLoadSecrets = mock(SecretProvider.class);
        when(redshiftLoadSecrets.getSecretOptional("access-key-id"))
                .thenReturn(Optional.of("test-redshift-load-access-key-id"));
        when(redshiftLoadSecrets.getSecretOptional("secret-access-key"))
                .thenReturn(Optional.of("test-redshift-load-secret-access-key"));

        SecretProvider awsSecrets = mock(SecretProvider.class);
        when(awsSecrets.getSecretOptional(anyString())).thenReturn(Optional.absent());
        when(awsSecrets.getSecrets("redshift")).thenReturn(redshiftSecrets);
        when(awsSecrets.getSecrets("redshift_load")).thenReturn(redshiftLoadSecrets);

        SecretProvider secrets = mock(SecretProvider.class);
        when(secrets.getSecrets("aws")).thenReturn(awsSecrets);

        Map.Entry<String, List<Object>> copyStatement =
                RedshiftLoadOperatorFactory.RedshiftLoadOperator.createCopyStatement(config, secrets);
        String sql = copyStatement.getKey();
        List<Object> params = copyStatement.getValue();
        System.out.println("sql: " + sql);
        System.out.println("params: " + params);
    }
}