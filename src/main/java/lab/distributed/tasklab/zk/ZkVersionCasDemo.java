package lab.distributed.tasklab.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ZkVersionCasDemo {

    private static final String PATH =
            "/distributed-task-lab/lab/persistent-demo";

    public static void main(String[] args) throws Exception {

        CuratorFramework clientA = createClient();
        CuratorFramework clientB = createClient();

        try {

            clientA.start();
            clientB.start();

            clientA.blockUntilConnected(
                    5,
                    TimeUnit.SECONDS
            );

            clientB.blockUntilConnected(
                    5,
                    TimeUnit.SECONDS
            );

            /*
             * Client A 读取
             */
            Stat statA = new Stat();

            byte[] dataA =
                    clientA.getData()
                            .storingStatIn(statA)
                            .forPath(PATH);

            System.out.println(
                    "Client A read:"
            );

            System.out.println(
                    "data    = "
                            + new String(
                                    dataA,
                                    StandardCharsets.UTF_8
                            )
            );

            System.out.println(
                    "version = " + statA.getVersion()
            );

            /*
             * Client B 也读取
             */
            Stat statB = new Stat();

            clientB.getData()
                    .storingStatIn(statB)
                    .forPath(PATH);

            System.out.println();
            System.out.println(
                    "Client B read version = "
                            + statB.getVersion()
            );

            /*
             * Client B 先修改
             */
            Stat updatedStat =
                    clientB.setData()
                            .withVersion(statB.getVersion())
                            .forPath(
                                    PATH,
                                    "updated-by-B"
                                            .getBytes(
                                                    StandardCharsets.UTF_8
                                            )
                            );

            System.out.println();
            System.out.println(
                    "Client B update success"
            );

            System.out.println(
                    "new version = "
                            + updatedStat.getVersion()
            );

            /*
             * Client A 仍使用之前读到的旧 version
             */
            try {

                clientA.setData()
                        .withVersion(statA.getVersion())
                        .forPath(
                                PATH,
                                "updated-by-A"
                                        .getBytes(
                                                StandardCharsets.UTF_8
                                        )
                        );

                System.out.println(
                        "Client A update success"
                );

            } catch (KeeperException.BadVersionException e) {

                System.out.println();
                System.out.println(
                        "Client A update FAILED: BadVersion"
                );

                System.out.println(
                        "Client A used stale version = "
                                + statA.getVersion()
                );
            }

            /*
             * 最后读取真实状态
             */
            Stat finalStat = new Stat();

            byte[] finalData =
                    clientA.getData()
                            .storingStatIn(finalStat)
                            .forPath(PATH);

            System.out.println();
            System.out.println(
                    "Final data    = "
                            + new String(
                                    finalData,
                                    StandardCharsets.UTF_8
                            )
            );

            System.out.println(
                    "Final version = "
                            + finalStat.getVersion()
            );

        } finally {

            clientA.close();
            clientB.close();
        }
    }

    private static CuratorFramework createClient() {

        return CuratorFrameworkFactory.newClient(
                "localhost:2181",
                15_000,
                5_000,
                new ExponentialBackoffRetry(
                        1000,
                        3
                )
        );
    }
}