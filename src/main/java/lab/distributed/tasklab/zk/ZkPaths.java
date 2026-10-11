package lab.distributed.tasklab.zk;

public final class ZkPaths {

    // 整个项目的根路径
    public static final String ROOT =
            "/distributed-task-lab";

    // 所有 Job 配置的父路径
    public static final String JOBS =
            ROOT + "/config/jobs";

    public static final String WORKERS =
            ROOT + "/runtime/workers";

    public static String workerPath(String workerId) {

        if (workerId == null ||
                !workerId.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException(
                    "Invalid workerId: " + workerId
            );
        }

        return WORKERS + "/" + workerId;
    }

    // 根据 jobId 生成 Job 节点路径
    public static String jobPath(String jobId) {

        if (jobId == null ||
                !jobId.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException(
                    "Invalid jobId: " + jobId
            );
        }

        return JOBS + "/" + jobId;
    }

    // 不允许实例化工具类
    private ZkPaths() {
    }
}