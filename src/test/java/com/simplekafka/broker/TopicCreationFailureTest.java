package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a client is told when a topic could not be created.
 *
 * <p>Creating a topic writes a ZooKeeper node and opens a log per partition, so it can
 * fail partway through. The client is waiting for an answer either way, and the one
 * answer it must not get is "created" - it would go on to produce into a topic that
 * does not exist on the broker that was supposed to be making it.
 */
class TopicCreationFailureTest {

    private static final int BROKER_ID = 1;
    private static final long TIMEOUT = 60_000;

    @TempDir
    static Path tempDir;

    private static final List<SimpleKafkaBroker> brokers = new ArrayList<>();
    private static NIOServerCnxnFactory zooKeeperFactory;
    private static ZooKeeperServer zooKeeperServer;
    private static int zooKeeperPort;
    private static int brokerPort;

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

        brokerPort = freePort();
        SimpleKafkaBroker broker = new SimpleKafkaBroker(BROKER_ID, "127.0.0.1", brokerPort, zooKeeperPort);
        broker.start();
        brokers.add(broker);

        awaitTrue("the broker to take controllership", () -> broker.isController());
    }

    @AfterAll
    static void stopCluster() {
        for (SimpleKafkaBroker broker : brokers) {
            try {
                broker.stop();
            } catch (Exception ignored) {
                // best effort teardown
            }
        }
        brokers.clear();

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

        File[] topics = new File("data/" + BROKER_ID).listFiles((dir, name) -> name.startsWith("uncreatable-"));
        if (topics != null) {
            for (File topic : topics) {
                deleteRecursively(topic);
            }
        }
    }

    /**
     * ZooKeeper refuses a path containing a NUL byte, which is a real failure coming out
     * of the first thing createTopic does - and one that needs no disk or cluster
     * gymnastics to arrange.
     */
    @Test
    void aTopicThatCouldNotBeCreatedIsNotReportedAsCreated() throws Exception {
        String topic = "uncreatable-" + UUID.randomUUID().toString().substring(0, 8) + "\u0000";

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", brokerPort));
            Protocol.writeFully(channel, Protocol.encodeCreateTopicRequest(topic, 1, (short) 1));

            ByteBuffer type = ByteBuffer.allocate(1);
            Protocol.readFully(channel, type, TIMEOUT);
            type.flip();

            assertEquals(Protocol.ERROR_RESPONSE, type.get(),
                    "a topic the broker could not create must not be answered with success");
        }
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
