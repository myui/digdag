package io.digdag.core.agent;

import java.util.List;
import java.time.Instant;
import java.io.IOException;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.core.log.TaskLogger;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.StorageObject;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;

public interface TaskCallbackApi
{
    TaskLogger newTaskLogger(TaskRequest request);

    void taskHeartbeat(int siteId,
            List<String> lockedIds, AgentId agentId, int lockSeconds);

    Optional<StorageObject> openArchive(TaskRequest request)
        throws IOException;

    void taskSucceeded(int siteId,
            long taskId, String lockId, AgentId agentId,
            TaskResult result);

    void taskFailed(int siteId,
            long taskId, String lockId, AgentId agentId,
            Config error);

    void retryTask(int siteId,
            long taskId, String lockId, AgentId agentId,
            int retryInterval, Config retryStateParams,
            Optional<Config> error);

    AttemptStateFlags startSession(
            int siteId,
            int projectId,
            String workflowName,
            Instant instant,
            Optional<String> retryAttemptName,
            Config overrideParams)
        throws ResourceNotFoundException, ResourceLimitExceededException;
}
