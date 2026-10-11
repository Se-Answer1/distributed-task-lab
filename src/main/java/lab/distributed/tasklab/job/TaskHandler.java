package lab.distributed.tasklab.job;

public interface TaskHandler {

    TaskResult execute(TaskContext context);

}