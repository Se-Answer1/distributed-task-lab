package lab.distributed.tasklab.worker;

import lab.distributed.tasklab.job.LoggingTaskHandler;
import lab.distributed.tasklab.job.TaskDefinition;
import lab.distributed.tasklab.job.TaskHandler;
import lab.distributed.tasklab.job.TaskResult;
import lab.distributed.tasklab.runtime.TaskRuntimeManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class WorkerMain {

    public static void main(String[] args) throws Exception {

        String workerId = "worker-1";

        // 1. 定义三个逻辑 Task
        List<TaskDefinition> tasks = List.of(

                new TaskDefinition(
                        "task-001",
                        "job-001",
                        "logging",
                        0,
                        3
                ),

                new TaskDefinition(
                        "task-002",
                        "job-001",
                        "logging",
                        1,
                        3
                ),

                new TaskDefinition(
                        "task-003",
                        "job-001",
                        "logging",
                        2,
                        3
                )
        );

        // 2. 创建 Worker 的线程池运行管理器
        int workerThreadCount = 2;

        TaskRuntimeManager runtimeManager =
                new TaskRuntimeManager(
                        workerId,
                        workerThreadCount
                );

        // 3. 指定任务处理器
        TaskHandler handler = new LoggingTaskHandler();

        List<Future<TaskResult>> futures = new ArrayList<>();

        long experimentStartedAt =
                System.currentTimeMillis();

        try {

            // 4. 将 Task 提交到线程池
            for (TaskDefinition task : tasks) {

                Future<TaskResult> future =
                        runtimeManager.submit(
                                task,
                                handler
                        );

                futures.add(future);
            }

            // 5. 等待 Task 执行完成
            for (Future<TaskResult> future : futures) {

                TaskResult result = future.get();

                System.out.println(
                        "Task finished: "
                                + result.getTaskId()
                                + ", success="
                                + result.isSuccess()
                );
            }

        } finally {

            // 6. 关闭线程池
            runtimeManager.shutdown();
        }

        long experimentFinishedAt =
                System.currentTimeMillis();

        System.out.println();
        System.out.println(
                "Total elapsed = "
                        + (experimentFinishedAt - experimentStartedAt)
                        + " ms"
        );
    }
}