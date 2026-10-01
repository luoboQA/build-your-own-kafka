package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.simplekafka.client.SimpleKafkaClient;

/**
 * What a client does when the broker it was talking to goes away.
 *
 * <p>A client caches which broker leads each partition and has no other way to find
 * out. Recovery used to be the application's job: the failover test in this suite calls
 * refreshMetadata() by hand, which is not something a long-lived client can rely on.
 */
class ClientRecoveryTest {

    private static final int FIRST = 1;
    private static final int SECOND = 2;
    private static final long TIMEOUT = 60_000;

    @TempDir
    static Path tempDir;

    private static final Map<Integer, SimpleKafkaBroker> brokers = new LinkedHashMap<>();
    private static final Map<Integer, Integer> brokerPorts = new LinkedHashMap<>();
    private static NIOServerCnxnFactory zooKeeperFactory;
    private static ZooKeeperServer zooKeeperServer;
    private static int zooKeeperPort;

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

        for (int brokerId : new int[] {FIRST, SECOND}) {
            int port = freePort();
            SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, "127.0.0.1", port, zooKeeperPort);
            broker.start();
            brokers.put(brokerId, broker);
            brokerPorts.put(brokerId, port);
        }
    }

    @AfterAll
    static void stopCluster() {
        for (SimpleKafkaBroker broker : new ArrayList<>(brokers.values())) {
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

        for (int brokerId : new int[] {FIRST, SECOND}) {
            File[] topics = new File("data/" + brokerId).listFiles((dir, name) -> name.startsWith("recover-"));
            if (topics != null) {
                for (File topic : topics) {
                    deleteRecursively(topic);
                }
            }
        }
    }

    /**
     * The bootstrap address is the client's way in, and it is easy to make it the only
     * one: the cluster can be perfectly healthy while the client that insists on that
     * one address can no longer refresh anything, including the address of a broker that
     * is still up.
     */
    @Test
    void metadataIsRefreshedFromAnotherBrokerWhenTheBootstrapIsGone() throws Exception {
        SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", portFor(SECOND));
        client.initialize();
        assertEquals(2, client.getBrokers().size(), "the client must know both brokers");

        stopBroker(SECOND);
        try {
            client.refreshMetadata();
            assertTrue(client.getBrokers().size() >= 1,
                    "the refresh must have gone to the broker that is still up");
        } finally {
            startBroker(SECOND);
        }
    }

    /**
     * A client that has cached a leader must recover on its own when that leader goes
     * away. Reaching a broker is the one failure that is unambiguously safe to retry -
     * the request never arrived - and refreshing is what finds the broker that leads now.
     */
    @Test
    void aProduceRecoversWhenTheLeaderItCachedIsGone() throws Exception {
        String topic = "recover-" + UUID.randomUUID().toString().substring(0, 8);
        SimpleKafkaClient setup = new SimpleKafkaClient("127.0.0.1", portFor(FIRST));
        setup.initialize();
        createTopic(setup, topic);

        // Bootstrap on the broker that is going to survive, so the client keeps a way in.
        SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", portFor(SECOND));
        client.initialize();

        int leader = leaderAt(portFor(SECOND), topic);
        assertEquals(0, client.send(topic, 0, bytes("before")));

        int follower = leader == FIRST ? SECOND : FIRST;
        stopBroker(leader);
        try {
            // The client still has the dead leader cached and is given no help.
            awaitTrue("the partition to be led again", TIMEOUT, () -> leaderAt(portFor(follower), topic) == follower);

            assertEquals(1, client.send(topic, 0, bytes("after")),
                    "the client must find the new leader by itself");
        } finally {
            startBroker(leader);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static void createTopic(SimpleKafkaClient client, String topic) throws Exception {
        awaitTrue("creation of topic " + topic, () -> {
            client.refreshMetadata();
            if (client.getTopicMetadata(topic) != null) {
                return true;
            }
            client.createTopic(topic, 1, (short) 2);
            client.refreshMetadata();
            return client.getTopicMetadata(topic) != null;
        });
    }

    private static int leaderAt(int port, String topic) throws Exception {
        try (java.nio.channels.SocketChannel channel = java.nio.channels.SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", port));
            Protocol.writeFully(channel, Protocol.encodeMetadataRequest());

            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(8192);
            long deadline = System.currentTimeMillis() + 30_000;
            long quietSince = -1;
            boolean wasBlocking = channel.isBlocking();
            channel.configureBlocking(false);
            try {
                while (System.currentTimeMillis() < deadline) {
                    int read = channel.read(buffer);
                    if (read < 0) {
                        break;
                    }
                    if (read > 0) {
                        quietSince = -1;
                        continue;
                    }
                    if (buffer.position() == 0) {
                        Thread.sleep(5);
                        continue;
                    }
                    if (quietSince < 0) {
                        quietSince = System.currentTimeMillis();
                    } else if (System.currentTimeMillis() - quietSince >= 150) {
                        break;
                    }
                    Thread.sleep(5);
                }
            } finally {
                buffer.flip();
                channel.configureBlocking(wasBlocking);
            }

            Protocol.MetadataResult metadata = Protocol.decodeMetadataResponse(buffer);
            for (Protocol.TopicMetadata topicMetadata : metadata.getTopics()) {
                if (topicMetadata.getName().equals(topic)) {
                    for (Protocol.PartitionMetadata partition : topicMetadata.getPartitions()) {
                        if (partition.getId() == 0) {
                            return partition.getLeader();
                        }
                    }
                }
            }
            return -1;
        }
    }

    private static void startBroker(int brokerId) throws Exception {
        int port = freePort();
        SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, "127.0.0.1", port, zooKeeperPort);
        broker.start();
        brokers.put(brokerId, broker);
        brokerPorts.put(brokerId, port);
    }

    private static void stopBroker(int brokerId) throws Exception {
        SimpleKafkaBroker broker = brokers.remove(brokerId);
        if (broker != null) {
            broker.stop();
        }
        brokerPorts.remove(brokerId);
    }

    private static int portFor(int brokerId) {
        Integer port = brokerPorts.get(brokerId);
        if (port == null) {
            throw new IllegalStateException("Broker " + brokerId + " is not running");
        }
        return port;
    }

    private static void awaitTrue(String what, Condition condition) throws Exception {
        awaitTrue(what, TIMEOUT, condition);
    }

    private static void awaitTrue(String what, long timeoutMillis, Condition condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.isMet()) {
                    return;
                }
            } catch (Exception ignored) {
                // keep waiting
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for " + what);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
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
