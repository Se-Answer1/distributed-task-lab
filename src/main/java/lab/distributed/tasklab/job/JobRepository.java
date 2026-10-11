
package lab.distributed.tasklab.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import lab.distributed.tasklab.zk.ZkPaths;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JobRepository {

    private final CuratorFramework client;
    private final ObjectMapper json = new ObjectMapper();

    public JobRepository(CuratorFramework client) {
        this.client = client;
    }

    // 创建任务配置
    public void create(JobDefinition job) throws Exception {
        long now = System.currentTimeMillis();

        JobDefinition toSave = withMeta(
                job,
                0,
                job.createdAt() > 0 ? job.createdAt() : now,
                now
        );

        byte[] data = json.writeValueAsBytes(toSave);

        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(ZkPaths.jobPath(job.jobId()), data);
    }

    // 查询任务配置，同时读取 ZooKeeper 版本号
    public JobDefinition get(String jobId) throws Exception {
        Stat stat = new Stat();

        byte[] data = client.getData()
                .storingStatIn(stat)
                .forPath(ZkPaths.jobPath(jobId));

        JobDefinition job =
                json.readValue(data, JobDefinition.class);

        return withMeta(
                job,
                stat.getVersion(),
                job.createdAt(),
                job.updatedAt()
        );
    }

    // 判断任务是否存在
    public boolean exists(String jobId) throws Exception {
        return client.checkExists()
                .forPath(ZkPaths.jobPath(jobId)) != null;
    }

    // 查询所有任务
    public List<JobDefinition> list() throws Exception {
        List<JobDefinition> jobs = new ArrayList<>();

        if (client.checkExists().forPath(ZkPaths.JOBS) == null) {
            return jobs;
        }

        List<String> jobIds = client.getChildren()
                .forPath(ZkPaths.JOBS);

        for (String jobId : jobIds) {
            try {
                jobs.add(get(jobId));
            } catch (KeeperException.NoNodeException e) {
                // 遍历过程中，节点可能被其他客户端删除
            }
        }

        return jobs;
    }

    // CAS 更新任务配置
    public JobDefinition update(JobDefinition job) throws Exception {
        if (job.version() < 0
                || job.version() >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Invalid job version: " + job.version()
            );
        }

        int expectedVersion = (int) job.version();
        long now = System.currentTimeMillis();

        JobDefinition updatedJob = withMeta(
                job,
                expectedVersion + 1L,
                job.createdAt(),
                now
        );

        byte[] data = json.writeValueAsBytes(updatedJob);

        Stat stat = client.setData()
                .withVersion(expectedVersion)
                .forPath(ZkPaths.jobPath(job.jobId()), data);

        return withMeta(
                updatedJob,
                stat.getVersion(),
                updatedJob.createdAt(),
                updatedJob.updatedAt()
        );
    }

    // 删除任务配置
    public void delete(String jobId) throws Exception {
        client.delete()
                .forPath(ZkPaths.jobPath(jobId));
    }

    // record 不可修改字段，通过创建新对象更新元数据
    private JobDefinition withMeta(
            JobDefinition job,
            long version,
            long createdAt,
            long updatedAt) {

        return new JobDefinition(
                job.jobId(),
                job.jobType(),
                job.handlerName(),
                job.enabled(),
                job.parallelism(),
                job.intervalSeconds(),
                job.maxRetries(),
                job.parameters(),
                version,
                createdAt,
                updatedAt
        );
    }

    // 独立测试入口
    public static void main(String[] args) throws Exception {

        try (CuratorFramework client =
                     CuratorFrameworkFactory.newClient(
                             "localhost:2181",
                             15_000,
                             5_000,
                             new ExponentialBackoffRetry(1000, 3)
                     )) {

            client.start();

            if (!client.blockUntilConnected(
                    5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("ZK not connected");
            }

            System.out.println("ZooKeeper connected");

            JobRepository repo = new JobRepository(client);

            String jobId = "job-001";

            // 1. 创建任务
            if (!repo.exists(jobId)) {
                long now = System.currentTimeMillis();

                JobDefinition job = new JobDefinition(
                        jobId,
                        "INTERVAL",
                        "logging",
                        true,
                        2,
                        5,
                        3,
                        Map.of("message", "Hello ZooKeeper"),
                        0,
                        now,
                        now
                );

                repo.create(job);
                System.out.println("Created: " + jobId);
            }

            // 2. 读取任务
            JobDefinition original = repo.get(jobId);

            System.out.println("Original job: " + original);
            System.out.println("Original version: "
                    + original.version());

            // 3. CAS 更新
            JobDefinition updated = repo.update(original);

            System.out.println("Updated version: "
                    + updated.version());

            // 4. 使用旧版本再次更新，预期失败
            try {
                repo.update(original);
                throw new IllegalStateException(
                        "CAS test failed: stale update succeeded"
                );
            } catch (KeeperException.BadVersionException e) {
                System.out.println(
                        "CAS success: stale version rejected"
                );
            }

            // 5. 查询所有任务
            System.out.println("All jobs:");

            for (JobDefinition job : repo.list()) {
                System.out.println("  " + job);
            }

            // 6. 删除测试任务
            String tempJobId = "job-delete-test";
            long now = System.currentTimeMillis();

            if (!repo.exists(tempJobId)) {
                JobDefinition tempJob = new JobDefinition(
                        tempJobId,
                        "INTERVAL",
                        "logging",
                        true,
                        1,
                        10,
                        0,
                        Map.of(),
                        0,
                        now,
                        now
                );

                repo.create(tempJob);
            }

            repo.delete(tempJobId);

            System.out.println("Delete success: "
                    + !repo.exists(tempJobId));

            System.out.println("JobRepository test finished");
        }
    }
}
