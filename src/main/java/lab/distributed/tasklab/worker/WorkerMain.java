package lab.distributed.tasklab.worker;

import lab.distributed.tasklab.job.JobDefinition;
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

        // -------------------------
        // 1. 定义三个 Task
        // -------------------------

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

        // -------------------------
        // 2. 定义 Job
        // -------------------------

        JobDefinition job = new JobDefinition(
                "job-001",
                tasks
        );

        // -------------------------
        // 3. Worker 自己的执行能力
        // -------------------------

        int workerThreadCount = 2;

        TaskRuntimeManager runtimeManager =
                new TaskRuntimeManager(
                        workerId,
                        workerThreadCount
                );

        TaskHandler handler =
                new LoggingTaskHandler();

        List<Future<TaskResult>> futures =
                new ArrayList<>();

        long experimentStartedAt =
                System.currentTimeMillis();

        try {

            // -------------------------
            // 4. 把 Job 中三个 Task
            //    全部提交给 Worker
            // -------------------------

            for (TaskDefinition task : job.getTasks()) {

                Future<TaskResult> future =
                        runtimeManager.submit(
                                task,
                                handler
                        );

                futures.add(future);
            }

            // -------------------------
            // 5. 等待所有 Task 完成
            // -------------------------

            for (Future<TaskResult> future : futures) {

                TaskResult result =
                        future.get();

                System.out.println(
                        "Task finished: "
                                + result.getTaskId()
                                + ", success="
                                + result.isSuccess()
                );
            }

        } finally {

            runtimeManager.shutdown();
        }

        long experimentFinishedAt =
                System.currentTimeMillis();

        System.out.println();
        System.out.println(
                "Total elapsed = "
                        + (experimentFinishedAt
                        - experimentStartedAt)
                        + " ms"
        );
    }
}