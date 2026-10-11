package lab.distributed.tasklab.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class ZkNodeObserverDemo {

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

            boolean exists =
                    client.checkExists()
                            .forPath(PATH) != null;

            System.out.println(
                    "Observed node exists = " + exists
            );

        } finally {

            client.close();
        }
    }
}