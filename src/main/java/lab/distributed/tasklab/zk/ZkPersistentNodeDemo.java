package lab.distributed.tasklab.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ZkPersistentNodeDemo {

    private static final String PATH =
            "/distributed-task-lab/lab/persistent-demo";

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

            Stat stat =
                    client.checkExists()
                            .forPath(PATH);

            if (stat == null) {

                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(
                                PATH,
                                "hello-zookeeper"
                                        .getBytes(StandardCharsets.UTF_8)
                        );

                System.out.println(
                        "Created persistent node: " + PATH
                );

            } else {

                System.out.println(
                        "Node already exists: " + PATH
                );
            }

            Stat readStat = new Stat();

            byte[] data =
                    client.getData()
                            .storingStatIn(readStat)
                            .forPath(PATH);

            System.out.println(
                    "data    = "
                            + new String(
                                    data,
                                    StandardCharsets.UTF_8
                            )
            );

            System.out.println(
                    "version = " + readStat.getVersion()
            );

            System.out.println(
                    "czxid   = " + readStat.getCzxid()
            );

            System.out.println(
                    "mzxid   = " + readStat.getMzxid()
            );

        } finally {

            client.close();
        }
    }
}