package lab.distributed.tasklab.runtime;

import lab.distributed.tasklab.job.TaskContext;
import lab.distributed.tasklab.job.TaskDefinition;
import lab.distributed.tasklab.job.TaskHandler;
import lab.distributed.tasklab.job.TaskResult;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TaskRuntimeManager {

    private final String workerId;
    private final ExecutorService executorService;

    public TaskRuntimeManager(String workerId,
                              int threadCount) {
        this.workerId = workerId;
        this.executorService =
                Executors.newFixedThreadPool(threadCount);
    }

    public Future<TaskResult> submit(
            TaskDefinition task,
            TaskHandler handler) {

        return executorService.submit(() -> {

            TaskContext context =
                    new TaskContext(workerId, task);

            return handler.execute(context);
        });
    }

    public void shutdown() {
        executorService.shutdown();
    }
}