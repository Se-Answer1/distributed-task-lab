package lab.distributed.tasklab.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class ZkConnectionDemo {

    public static void main(String[] args) throws Exception {

        String connectString = "localhost:2181";

        int sessionTimeoutMs = 15_000;
        int connectionTimeoutMs = 5_000;

        RetryPolicy retryPolicy =
                new ExponentialBackoffRetry(
                        1000,
                        3
                );

        CuratorFramework client =
                CuratorFrameworkFactory.newClient(
                        connectString,
                        sessionTimeoutMs,
                        connectionTimeoutMs,
                        retryPolicy
                );

        client.getConnectionStateListenable()
                .addListener((curator, newState) -> {

                    System.out.println(
                            "ZooKeeper connection state = "
                                    + newState
                    );

                });

        try {

            System.out.println("Starting Curator client...");

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

            System.out.println(
                    "Connected to ZooKeeper successfully."
            );

            System.out.println(
                    "Press Enter to close the ZooKeeper session..."
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