package lab.distributed.tasklab.job;

public class TaskDefinition {

    private final String taskId;
    private final String jobId;
    private final String taskType;
    private final int shardIndex;
    private final int shardTotal;

    public TaskDefinition(String taskId,
                          String jobId,
                          String taskType,
                          int shardIndex,
                          int shardTotal) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.taskType = taskType;
        this.shardIndex = shardIndex;
        this.shardTotal = shardTotal;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskType() {
        return taskType;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public int getShardTotal() {
        return shardTotal;
    }
}