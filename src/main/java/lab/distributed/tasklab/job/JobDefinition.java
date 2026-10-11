package lab.distributed.tasklab.job;

import java.util.Map;

public record JobDefinition(
        String jobId,
        String jobType,
        String handlerName,
        boolean enabled,
        int parallelism,
        int intervalSeconds,
        int maxRetries,
        Map<String, String> parameters,
        long version,
        long createdAt,
        long updatedAt
) {
    // 参数校验
}