package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.simplekafka.client.SimpleKafkaClient;

/**
 * When the leader of a partition dies, the replacement has to come from that
 * partition's own replica set, and every broker has to be told.
 *
 * <p>Three brokers are started, but only two of them are assigned the topic: broker 1
 * joins afterwards, so it holds the assignment and answers metadata while owning no
 * log at all. That is the case that used to break - taking whichever broker happened
 * to sort first as the new leader would hand the partition to a broker whose log did
 * not exist, and only the controller would even know the leader had changed.
 */
class LeaderFailoverTest {

    private static final long TIMEOUT = 60_000;

    @TempDir
    static Path tempDir;

    private static final Map<Integer, SimpleKafkaBroker> brokers = new LinkedHashMap<>();
    private static final Map<Integer, Integer> brokerPorts = new LinkedHashMap<>();
    private static NIOServerCnxnFactory zooKeeperFactory;
    private static ZooKeeperServer zooKeeperServer;
    private static int zooKeeperPort;
    private static SimpleKafkaClient client;
    private static String topic;

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

        // Brokers 2 and 3 come up first, so the topic gets assigned to the two of them.
        startBroker(2);
        startBroker(3);

        SimpleKafkaClient bootstrap = new SimpleKafkaClient("127.0.0.1", portFor(2));
        bootstrap.initialize();

        awaitTrue("brokers 2 and 3 to see each other", TIMEOUT, () ->
                brokerIdsAt(portFor(2)).size() == 2 && brokerIdsAt(portFor(3)).size() == 2);

        topic = "fail-" + UUID.randomUUID().toString().substring(0, 8);
        createTopic(bootstrap, topic, 1, (short) 2);

        // Now bring in a broker the assignment never mentioned.
        startBroker(1);
        awaitTrue("all three brokers to see each other", TIMEOUT, () -> {
            for (int brokerId : new int[] {1, 2, 3}) {
                if (brokerIdsAt(portFor(brokerId)).size() != 3) {
                    return false;
                }
            }
            return true;
        });
        awaitTrue("the latecomer to load the topic", TIMEOUT, () -> brokerKnowsTopic(portFor(1), topic));

        // Bootstrap the client on broker 1: it is the one broker the failover below
        // does not take away, so the client never loses its way into the cluster.
        client = new SimpleKafkaClient("127.0.0.1", portFor(1));
        client.initialize();
    }

    @AfterAll
    static void stopCluster() throws Exception {
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

        for (int brokerId : new int[] {1, 2, 3}) {
            File[] topics = new File("data/" + brokerId).listFiles((dir, name) -> name.startsWith("fail-"));
            if (topics != null) {
                for (File staleTopic : topics) {
                    deleteRecursively(staleTopic);
                }
            }
        }
    }

    @Test
    void aDeadLeaderIsReplacedByASurvivingReplicaThatEveryBrokerAgreesOn() throws Exception {
        // The setup matters more than it looks: only {2, 3} replicate this partition,
        // and 1 is the lowest-numbered broker still running once the leader is gone.
        List<Integer> replicas = replicasAt(portFor(1), topic, 0);
        assertTrue(replicas.containsAll(List.of(2, 3)) && replicas.size() == 2,
                "the topic must be assigned to brokers 2 and 3, but was " + replicas);

        int oldLeader = leaderAt(portFor(1), topic, 0);
        assertTrue(replicas.contains(oldLeader), "leader " + oldLeader + " is not one of " + replicas);
        int survivor = replicas.stream().filter(id -> id != oldLeader).findFirst().orElseThrow();

        assertEquals(0, client.send(topic, 0, bytes("before-the-failover")));
        awaitTrue("both replicas to hold the message", TIMEOUT, () ->
                readLog(2, topic, 0).size() == 1 && readLog(3, topic, 0).size() == 1);

        stopBroker(oldLeader);

        // 1. The controller must elect from the replica set. Broker 1 is outside it
        //    and, being the lowest-numbered broker still up, is exactly the one the
        //    old "take the first broker in the map" logic reached for - with an empty
        //    log to lead from.
        awaitTrue("the surviving replica to take over", TIMEOUT, () ->
                leaderAt(portFor(1), topic, 0) == survivor);

        // 2. The new leader has to be told too, or it goes on forwarding writes to the
        //    broker that just died. Both assertions are needed: whichever broker did
        //    not win the controller seat is the one that only has the watch to go on.
        assertEquals(survivor, leaderAt(portFor(survivor), topic, 0),
                "the new leader must learn that it leads");

        // 3. Learning the assignment must not conjure up a log on a non-replica.
        assertFalse(new File("data/1/" + topic).exists(),
                "broker 1 is not a replica of " + topic + " but has data/1/" + topic);

        // 4. Writes keep working, at the offset the old leader would have handed out.
        awaitTrue("the client to learn the new leader", TIMEOUT, () -> {
            client.refreshMetadata();
            SimpleKafkaClient.TopicMetadata metadata = client.getTopicMetadata(topic);
            return metadata != null && !metadata.getPartitions().isEmpty()
                    && metadata.getPartitions().get(0).getLeader() == survivor;
        });
        assertEquals(1, client.send(topic, 0, bytes("after-the-failover")));

        SortedMap<Long, String> log = readLog(survivor, topic, 0);
        assertEquals("before-the-failover", log.get(0L), "offset 0 must survive the failover");
        assertEquals("after-the-failover", log.get(1L));

        // 5. When the old leader returns it rejoins as a follower; leadership only
        //    moves when a replica actually stops being reachable.
        startBroker(oldLeader);
        awaitTrue("every broker to be back", TIMEOUT, () -> brokerIdsAt(portFor(1)).size() == 3);
        awaitTrue("the restarted broker to answer metadata", TIMEOUT, () ->
                leaderAt(portFor(oldLeader), topic, 0) == survivor);
        assertEquals(survivor, leaderAt(portFor(1), topic, 0),
                "a restarted broker must not seize the partition it no longer leads");
    }

    // ------------------------------------------------------------------ helpers

    private static void createTopic(SimpleKafkaClient bootstrap, String name, int partitions, short replicationFactor)
            throws Exception {
        awaitTrue("creation of topic " + name, TIMEOUT, () -> {
            bootstrap.refreshMetadata();
            if (bootstrap.getTopicMetadata(name) != null) {
                return true;
            }
            bootstrap.createTopic(name, partitions, replicationFactor);
            bootstrap.refreshMetadata();
            return bootstrap.getTopicMetadata(name) != null;
        });
    }

    /**
     * What one specific broker claims, rather than whichever broker answered first -
     * failover only works if they all end up saying the same thing.
     */
    private static int leaderAt(int port, String topic, int partition) throws IOException {
        for (Protocol.TopicMetadata topicMetadata : fetchMetadata(port).getTopics()) {
            if (!topicMetadata.getName().equals(topic)) {
                continue;
            }
            for (Protocol.PartitionMetadata partitionMetadata : topicMetadata.getPartitions()) {
                if (partitionMetadata.getId() == partition) {
                    return partitionMetadata.getLeader();
                }
            }
        }
        throw new IOException("Broker on port " + port + " does not know topic " + topic);
    }

    private static List<Integer> replicasAt(int port, String topic, int partition) throws IOException {
        for (Protocol.TopicMetadata topicMetadata : fetchMetadata(port).getTopics()) {
            if (!topicMetadata.getName().equals(topic)) {
                continue;
            }
            for (Protocol.PartitionMetadata partitionMetadata : topicMetadata.getPartitions()) {
                if (partitionMetadata.getId() == partition) {
                    List<Integer> replicas = new ArrayList<>(partitionMetadata.getReplicas());
                    replicas.add(partitionMetadata.getLeader());
                    return replicas;
                }
            }
        }
        throw new IOException("Broker on port " + port + " does not know topic " + topic);
    }

    private static boolean brokerKnowsTopic(int port, String topic) throws IOException {
        try {
            for (Protocol.TopicMetadata topicMetadata : fetchMetadata(port).getTopics()) {
                if (topicMetadata.getName().equals(topic)) {
                    return true;
                }
            }
        } catch (IOException | RuntimeException e) {
            return false;
        }
        return false;
    }

    private static Protocol.MetadataResult fetchMetadata(int port) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", port));
            Protocol.writeFully(channel, Protocol.encodeMetadataRequest());
            return Protocol.decodeMetadataResponse(readUntilQuiet(channel));
        }
    }

    private static List<Integer> brokerIdsAt(int port) throws IOException {
        List<Integer> ids = new ArrayList<>();
        for (BrokerInfo broker : fetchMetadata(port).getBrokers()) {
            ids.add(broker.getId());
        }
        return ids;
    }

    /**
     * The metadata response has no length prefix, so read until the stream goes quiet.
     */
    private static ByteBuffer readUntilQuiet(SocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        boolean wasBlocking = channel.isBlocking();
        channel.configureBlocking(false);
        try {
            long deadline = System.currentTimeMillis() + 30_000;
            long quietSince = -1;
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
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading the metadata response", e);
        } finally {
            buffer.flip();
            channel.configureBlocking(wasBlocking);
        }
        return buffer;
    }

    /**
     * Read a partition straight off disk, so the assertion is about what the broker
     * stored rather than what it claims through the API.
     */
    private static SortedMap<Long, String> readLog(int brokerId, String topic, int partition) throws IOException {
        File dir = new File("data/" + brokerId + "/" + topic + "/" + partition);
        SortedMap<Long, String> entries = new TreeMap<>();

        File[] segments = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (segments == null) {
            return entries;
        }

        for (File segment : segments) {
            long baseOffset = Long.parseLong(segment.getName().substring(0, segment.getName().length() - 4));
            byte[] data = Files.readAllBytes(segment.toPath());

            int position = 0;
            long offset = baseOffset;
            while (position + 4 <= data.length) {
                int size = ByteBuffer.wrap(data, position, 4).getInt();
                position += 4;
                if (size < 0 || position + size > data.length) {
                    break;
                }
                entries.put(offset++, new String(data, position, size, StandardCharsets.UTF_8));
                position += size;
            }
        }

        return entries;
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
    }

    private static int portFor(int brokerId) {
        Integer port = brokerPorts.get(brokerId);
        if (port == null) {
            throw new IllegalStateException("Broker " + brokerId + " has never run in this test");
        }
        return port;
    }

    private static void awaitTrue(String what, long timeoutMillis, Condition condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.isMet()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for " + what);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static int freePort() throws IOException {
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
        //noinspection ResultOfMethodCallIgnored
        file.delete();
    }
}
