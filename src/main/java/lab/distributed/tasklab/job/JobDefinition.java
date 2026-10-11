package lab.distributed.tasklab.job;

import java.util.List;

public class JobDefinition {

    private final String jobId;
    private final boolean enabled;
    private final int parallelism;
    private final List<TaskDefinition> tasks;

    // 兼容之前 Stage 1 的代码
    public JobDefinition(String jobId,
                         List<TaskDefinition> tasks) {

        this(jobId, true, tasks.size(), tasks);
    }

    // 新的构造方法
    public JobDefinition(String jobId,
                         boolean enabled,
                         int parallelism,
                         List<TaskDefinition> tasks) {

        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId is empty");
        }

        if (parallelism < 1) {
            throw new IllegalArgumentException(
                    "parallelism must be >= 1");
        }

        this.jobId = jobId;
        this.enabled = enabled;
        this.parallelism = parallelism;
        this.tasks = List.copyOf(tasks);
    }

    public String getJobId() {
        return jobId;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getParallelism() {
        return parallelism;
    }

    public List<TaskDefinition> getTasks() {
        return tasks;
    }
}