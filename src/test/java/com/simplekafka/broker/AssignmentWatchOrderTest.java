package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The order in which a partition's assignment is watched and read.
 *
 * <p>A ZooKeeper watch fires once. If the next one is armed after the assignment has
 * been read, any change that lands in between produces no event here at all - and this
 * watch is the only way a broker learns a partition's leader moved, so it would go on
 * answering metadata with, and forwarding writes to, a broker that is gone. Nothing
 * afterwards corrects it.
 *
 * <p>The ordering is invisible from outside the broker, so the broker is built around a
 * client that records what it is asked to do.
 */
class AssignmentWatchOrderTest {

    private static final int BROKER_ID = 1;
    private static final long TIMEOUT = 60_000;

    @TempDir
    static Path tempDir;

    private static NIOServerCnxnFactory zooKeeperFactory;
    private static ZooKeeperServer zooKeeperServer;
    private static int zooKeeperPort;

    private static RecordingZookeeperClient recorder;
    private static SimpleKafkaBroker broker;

    @FunctionalInterface
    private interface Condition {
        boolean isMet() throws Exception;
    }

    @BeforeAll
    static void startCluster() throws Exception {
        zooKeeperPort = freePort();
        Path zooKeeperData = Files.createDirectories(tempDir.resolve("zookeeper"));
        zooKeeperServer = new ZooKeeperServer(zooKeeperData.toFile(), zooKeeperData.toFile(), 2000);
        zooKeeperFactory = new NIOServerCnxnFactory();
        zooKeeperFactory.configure(new InetSocketAddress("127.0.0.1", zooKeeperPort), 100);
        zooKeeperFactory.startup(zooKeeperServer);

        recorder = new RecordingZookeeperClient("localhost", zooKeeperPort);
        broker = new SimpleKafkaBroker(BROKER_ID, "127.0.0.1", freePort(), recorder);
        broker.start();

        awaitTrue("the broker to take controllership", () -> broker.isController());
    }

    @AfterAll
    static void stopCluster() {
        try {
            broker.stop();
        } catch (Exception ignored) {
            // best effort teardown
        }
        try {
            zooKeeperFactory.shutdown();
        } catch (Exception ignored) {
            // best effort teardown
        }
        try {
            zooKeeperServer.shutdown();
        } catch (Exception ignored) {
            // best effort teardown
        }

        File[] topics = new File("data/" + BROKER_ID).listFiles((dir, name) -> name.startsWith("watchorder-"));
        if (topics != null) {
            for (File topic : topics) {
                deleteRecursively(topic);
            }
        }
    }

    @Test
    void anAssignmentIsWatchedBeforeItIsRead() throws Exception {
        String topic = "watchorder-" + UUID.randomUUID().toString().substring(0, 8);
        String path = "/topics/" + topic + "/partitions/0";

        // Write the topic's nodes directly and load it, which arms the first watch.
        try (ZooKeeper testClient = connectAsTestClient()) {
            byte[] empty = new byte[0];
            testClient.create("/topics/" + topic, empty, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            testClient.create("/topics/" + topic + "/partitions", empty,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            testClient.create(path, "1;".getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        broker.loadTopic(topic);
        recorder.order.clear();

        // Touch the node, so the armed watch fires and the notification is handled.
        try (ZooKeeper testClient = connectAsTestClient()) {
            testClient.setData(path, "1;".getBytes(StandardCharsets.UTF_8), -1);
        }

        awaitTrue("the notification to be handled", () -> recorded("read " + path) >= 0);

        int watchAt = recorded("watch " + path);
        int readAt = recorded("read " + path);

        assertTrue(watchAt >= 0, "the notification must arm the next watch");
        assertTrue(watchAt < readAt,
                "the next watch must be armed before the assignment is read, or a change "
                        + "landing in between is lost for good; the broker did: " + recorder.order);
    }

    private static int recorded(String entry) {
        synchronized (recorder.order) {
            return recorder.order.indexOf(entry);
        }
    }

    /**
     * A broker's ZooKeeper client that writes down what the broker asks it for, in order,
     * and otherwise behaves exactly like the real one.
     */
    private static final class RecordingZookeeperClient extends ZookeeperClient {
        private final List<String> order = Collections.synchronizedList(new ArrayList<>());

        RecordingZookeeperClient(String host, int port) {
            super(host, port);
        }

        @Override
        public void watchNode(String path, NodeCallback callback) {
            order.add("watch " + path);
            super.watchNode(path, callback);
        }

        @Override
        public String readDataIfPresent(String path) throws KeeperException, InterruptedException {
            order.add("read " + path);
            return super.readDataIfPresent(path);
        }
    }

    private static ZooKeeper connectAsTestClient() throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:" + zooKeeperPort, 15_000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (!connected.await(15, TimeUnit.SECONDS)) {
            zooKeeper.close();
            fail("The test's ZooKeeper client did not connect");
        }
        return zooKeeper;
    }

    private static void awaitTrue(String what, Condition condition) throws Exception {
        long deadline = System.currentTimeMillis() + TIMEOUT;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.isMet()) {
                    return;
                }
            } catch (Exception ignored) {
                // keep waiting
            }
            Thread.sleep(50);
        }
        fail("Timed out waiting for " + what);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        if (!file.delete()) {
            file.deleteOnExit();
        }
    }
}
