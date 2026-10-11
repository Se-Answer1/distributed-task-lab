package lab.distributed.tasklab.job;

import java.time.Instant;

public class LoggingTaskHandler implements TaskHandler {

    @Override
    public TaskResult execute(TaskContext context) {

        TaskDefinition task = context.getTask();

        long processId =
                ProcessHandle.current().pid();

        String threadName =
                Thread.currentThread().getName();

        Instant startedAt = Instant.now();

        System.out.println("====================================");
        System.out.println("processId  = " + processId);
        System.out.println("workerId   = " + context.getWorkerId());
        System.out.println("jobId      = " + task.getJobId());
        System.out.println("taskId     = " + task.getTaskId());
        System.out.println("taskType   = " + task.getTaskType());
        System.out.println("shardIndex = " + task.getShardIndex());
        System.out.println("shardTotal = " + task.getShardTotal());
        System.out.println("threadName = " + threadName);
        System.out.println("startedAt  = " + startedAt);
        System.out.println("====================================");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            return new TaskResult(
                    task.getTaskId(),
                    false,
                    "interrupted"
            );
        }

        return new TaskResult(
                task.getTaskId(),
                true,
                "completed"
        );
    }
}