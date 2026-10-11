package lab.distributed.tasklab.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.Watcher;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ZkWatchDemo {

    private static final String PATH =
            "/distributed-task-lab/lab/persistent-demo";

    public static void main(String[] args) throws Exception {

        // 1. 创建 ZooKeeper 客户端
        CuratorFramework client =
                CuratorFrameworkFactory.newClient(
                        "localhost:2181",
                        15_000,
                        5_000,
                        new ExponentialBackoffRetry(1000, 3)
                );

        try {
            // 2. 启动客户端
            client.start();

            boolean connected =
                    client.blockUntilConnected(5, TimeUnit.SECONDS);

            if (!connected) {
                throw new IllegalStateException("ZK connection failed");
            }

            System.out.println("CONNECTED");

            // 3. 定义 Watcher：收到事件时执行
            Watcher watcher = new Watcher() {

                @Override
                public void process(org.apache.zookeeper.WatchedEvent event) {

                    System.out.println();
                    System.out.println("===== WATCH EVENT =====");
                    System.out.println("type = " + event.getType());
                    System.out.println("path = " + event.getPath());
                    System.out.println("thread = " +
                            Thread.currentThread().getName());

                    if (event.getType() !=
                            Watcher.Event.EventType.NodeDataChanged) {
                        return;
                    }

                    // 收到数据变更事件后，再次注册 Watch
                    try {
                        client.getData()
                                .usingWatcher(this)
                                .inBackground((c, result) -> {

                                    if (result.getResultCode() == 0) {
                                        System.out.println("Watch re-registered!");

                                        System.out.println(
                                                "Latest data = " +
                                                        new String(
                                                                result.getData(),
                                                                StandardCharsets.UTF_8
                                                        )
                                        );
                                    } else {
                                        System.out.println(
                                                "Re-register failed, code = " +
                                                        result.getResultCode()
                                        );
                                    }
                                })
                                .forPath(PATH);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            // 4. 读取节点数据，同时注册 Watch
            byte[] data = client.getData()
                    .usingWatcher(watcher)
                    .forPath(PATH);

            System.out.println(
                    "Current data = " +
                    new String(data, StandardCharsets.UTF_8)
            );

            System.out.println("Watch registered: " + PATH);

            // 5. 保持 JVM 主线程等待
            System.out.println("Press Enter to exit...");
            System.in.read();

        } finally {
            // 6. 关闭客户端
            client.close();
        }
    }
}