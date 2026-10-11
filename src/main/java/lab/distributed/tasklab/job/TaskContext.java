package lab.distributed.tasklab.job;

public class TaskContext {

    private final String workerId;
    private final TaskDefinition task;

    public TaskContext(String workerId,
                       TaskDefinition task) {
        this.workerId = workerId;
        this.task = task;
    }

    public String getWorkerId() {
        return workerId;
    }

    public TaskDefinition getTask() {
        return task;
    }
}