package io.digdag.standards.operator.jdbc;

import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.spi.*;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractJdbcOperator<C>
    extends BaseOperator
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final TemplateEngine templateEngine;

    public AbstractJdbcOperator(Path projectPath, TaskRequest request, TemplateEngine templateEngine)
    {
        super(projectPath, request);
        this.templateEngine = checkNotNull(templateEngine, "templateEngine");
    }

    protected abstract C configure(SecretProvider secrets, Config params);

    protected abstract JdbcConnection connect(C connectionConfig);

    protected abstract String type();


    protected abstract TaskResult run(TaskExecutionContext ctx, Config params, Config state, C connectionConfig);

    @Override
    public List<String> secretSelectors()
    {
        return ImmutableList.of(type() + ".*");
    }

    protected boolean strictTransaction(Config params)
    {
        return params.get("strict_transaction", Boolean.class, true);
    }

    @Override
    public TaskResult runTask(TaskExecutionContext ctx)
    {
        Config params = request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty(type()));
        Config state = request.getLastStateParams().deepCopy();
        C connectionConfig = configure(ctx.secrets().getSecrets(type()), params);
        return run(ctx, params, state, connectionConfig);
    }
}
