package lab.distributed.tasklab.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ZkEphemeralNodeDemo {

    private static final String PATH =
            "/distributed-task-lab/lab/ephemeral-demo";

    public static void main(String[] args) throws Exception {

        CuratorFramework client =
                CuratorFrameworkFactory.newClient(
                        "localhost:2181",
                        15_000,
                        5_000,
                        new ExponentialBackoffRetry(1000, 3)
                );

        try {

            client.start();

            boolean connected =
                    client.blockUntilConnected(
                            5,
                            TimeUnit.SECONDS
                    );

            if (!connected) {
                throw new IllegalStateException(
                        "Could not connect to ZooKeeper"
                );
            }

            if (client.checkExists().forPath(PATH) == null) {

                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(
                                PATH,
                                "worker-like-node"
                                        .getBytes(StandardCharsets.UTF_8)
                        );

                System.out.println(
                        "Created ephemeral node: " + PATH
                );
            }

            System.out.println(
                    "Node exists = "
                            + (client.checkExists().forPath(PATH) != null)
            );

            System.out.println();
            System.out.println(
                    "Session is alive."
            );

            System.out.println(
                    "Press Enter to close Curator client..."
            );

            System.in.read();

        } finally {

            System.out.println(
                    "Closing Curator client..."
            );

            client.close();
        }
    }
}