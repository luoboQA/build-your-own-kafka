package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.simplekafka.client.SimpleKafkaClient;

/**
 * End-to-end check that replicas hold the same messages at the same offsets.
 *
 * <p>Starts a real (embedded) ZooKeeper and two brokers, then compares the log
 * files the brokers actually keep on disk.
 */
class ReplicationOffsetTest {

    private static final int[] BROKER_IDS = {1, 2};
    private static final long TIMEOUT = 60_000;

    @TempDir
    static Path tempDir;

    private static final Map<Integer, SimpleKafkaBroker> brokers = new LinkedHashMap<>();
    private static final Map<Integer, Integer> brokerPorts = new LinkedHashMap<>();
    private static NIOServerCnxnFactory zooKeeperFactory;
    private static ZooKeeperServer zooKeeperServer;
    private static int zooKeeperPort;
    private static SimpleKafkaClient client;

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

        for (int brokerId : BROKER_IDS) {
            startBroker(brokerId);
        }

        client = new SimpleKafkaClient("127.0.0.1", portFor(BROKER_IDS[0]));
        client.initialize();

        awaitTrue("every broker to see every other broker", TIMEOUT, () -> {
            for (int brokerId : BROKER_IDS) {
                if (brokerIdsAt(portFor(brokerId)).size() != BROKER_IDS.length) {
                    return false;
                }
            }
            return true;
        });
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

        for (int brokerId : BROKER_IDS) {
            File[] topics = new File("data/" + brokerId).listFiles((dir, name) -> name.startsWith("repl-"));
            if (topics != null) {
                for (File topic : topics) {
                    deleteRecursively(topic);
                }
            }
        }
    }

    @Test
    void followersHoldTheSameMessagesAtTheSameOffsetsAsTheLeader() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 2);

        int leader = leaderOf(topic, 0);
        int follower = other(leader);

        List<String> sent = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            sent.add("msg-" + i);
        }
        for (int i = 0; i < sent.size(); i++) {
            long offset = client.send(topic, 0, bytes(sent.get(i)));
            assertEquals(i, offset, "the leader must assign offsets sequentially");
        }

        awaitTrue("replica logs to converge", TIMEOUT, () -> {
            SortedMap<Long, String> leaderLog = readLog(leader, topic, 0);
            SortedMap<Long, String> followerLog = readLog(follower, topic, 0);
            return leaderLog.size() == sent.size() && followerLog.equals(leaderLog);
        });

        SortedMap<Long, String> leaderLog = readLog(leader, topic, 0);
        SortedMap<Long, String> followerLog = readLog(follower, topic, 0);

        assertEquals(sent.size(), leaderLog.size());
        assertEquals(leaderLog, followerLog, "both replicas must hold identical logs");
        for (int i = 0; i < sent.size(); i++) {
            assertEquals(sent.get(i), leaderLog.get((long) i));
            assertEquals(sent.get(i), followerLog.get((long) i));
        }
    }

    @Test
    void followerRefusesToWriteAheadOfItsLogAndReportsWhereItStopped() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 2);
        produceAndAwaitReplication(topic, 3);

        int follower = other(leaderOf(topic, 0));
        SortedMap<Long, String> before = readLog(follower, topic, 0);
        long logEndOffset = before.size();

        byte[] reply = sendReplicate(portFor(follower), topic, 0, logEndOffset + 5, bytes("from-the-future"));

        assertEquals((int) Protocol.REPLICATE_NACK, (int) reply[0], "a gap must be refused, not papered over");
        assertEquals(logEndOffset, logEndOffset(reply), "the follower must report where its log really stops");
        assertEquals(before, readLog(follower, topic, 0), "a refused request must not touch the log");
    }

    @Test
    void followerWritesAtTheOffsetTheLeaderAssignsEvenWhenItsOwnCounterIsElsewhere() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 2);
        produceAndAwaitReplication(topic, 3);

        int leader = leaderOf(topic, 0);
        int follower = other(leader);

        // The follower's own log end offset is 3, but the leader says this message
        // belongs at offset 3 anyway: it must land exactly there.
        byte[] reply = sendReplicate(portFor(follower), topic, 0, 3, bytes("injected"));
        assertEquals((int) Protocol.REPLICATE_ACK, (int) reply[0]);
        assertEquals("injected", readLog(follower, topic, 0).get(3L));

        // The leader's next message is also assigned offset 3, so the injected copy
        // diverges from it and has to be truncated away.
        long offset = client.send(topic, 0, bytes("after-injection"));
        assertEquals(3, offset, "the leader still owns the offset sequence");

        awaitTrue("the diverged replica to be repaired", TIMEOUT, () -> {
            SortedMap<Long, String> leaderLog = readLog(leader, topic, 0);
            SortedMap<Long, String> followerLog = readLog(follower, topic, 0);
            return leaderLog.size() == 4 && followerLog.equals(leaderLog);
        });

        assertEquals("after-injection", readLog(follower, topic, 0).get(3L));
    }

    @Test
    void leaderCatchesAFollowerUpAfterItMissedMessages() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 2);
        produceAndAwaitReplication(topic, 3);

        int leader = leaderOf(topic, 0);
        int follower = other(leader);

        // Take the follower away: its replication writes are skipped, so the leader
        // pulls ahead while the follower keeps the log it already had.
        stopBroker(follower);
        awaitTrue("the follower to drop out of the leader's metadata", TIMEOUT, () ->
                brokerIdsAt(portFor(leader)).stream().noneMatch(id -> id == follower));

        for (int i = 3; i < 8; i++) {
            client.send(topic, 0, bytes("msg-" + i));
        }
        assertEquals(8, readLog(leader, topic, 0).size());
        assertEquals(3, readLog(follower, topic, 0).size());

        // Bring it back on a new port; it reloads the topic from ZooKeeper and keeps
        // the log it had, so it is now five messages behind the leader.
        startBroker(follower);
        client = new SimpleKafkaClient("127.0.0.1", portFor(BROKER_IDS[0]));
        client.initialize();

        awaitTrue("the follower to reappear in the leader's metadata on its new port", TIMEOUT, () -> {
            for (BrokerInfo broker : brokersAt(portFor(leader))) {
                if (broker.getId() == follower) {
                    return broker.getPort() == portFor(follower);
                }
            }
            return false;
        });

        long offset = client.send(topic, 0, bytes("msg-8"));
        assertEquals(8, offset);

        awaitTrue("the lagging follower to catch up", TIMEOUT, () -> {
            SortedMap<Long, String> leaderLog = readLog(leader, topic, 0);
            SortedMap<Long, String> followerLog = readLog(follower, topic, 0);
            return leaderLog.size() == 9 && followerLog.equals(leaderLog);
        });

        SortedMap<Long, String> followerLog = readLog(follower, topic, 0);
        for (int i = 0; i <= 8; i++) {
            assertEquals("msg-" + i, followerLog.get((long) i), "offset " + i + " on the caught-up replica");
        }
    }

    @Test
    void aBrokerOutsideTheReplicaSetKeepsTheAssignmentButNoLogFile() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 1);   // exactly one replica

        int leader = leaderOf(topic, 0);
        int outsider = other(leader);

        // Both brokers must have finished loading the topic before we assert what
        // loading it did to their data directories.
        awaitTrue("the leader to know topic " + topic, TIMEOUT, () -> brokerKnowsTopic(portFor(leader), topic));
        awaitTrue("the non-replica to know topic " + topic, TIMEOUT, () -> brokerKnowsTopic(portFor(outsider), topic));

        for (int i = 0; i < 3; i++) {
            assertEquals(i, client.send(topic, 0, bytes("msg-" + i)));
        }
        assertEquals(3, readLog(leader, topic, 0).size(), "the only replica must store the messages");

        // The non-replica still answers metadata requests (proven above), but it
        // must not own a log: an empty one would start at offset 0 while the real
        // replica is already at offset 3.
        File outsiderTopicDir = new File("data/" + outsider + "/" + topic);
        assertFalse(outsiderTopicDir.exists(),
                "broker " + outsider + " is not a replica of " + topic + " but has " + outsiderTopicDir);
    }

    @Test
    void onlyTheLeaderServesReads() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 2);

        int leader = leaderOf(topic, 0);
        int follower = other(leader);
        client.send(topic, 0, bytes("only-the-leader-answers-reads"));

        Protocol.FetchResult servedByLeader = fetchFrom(portFor(leader), topic, 0, 0, 4096);
        assertTrue(servedByLeader.isSuccess(), "the leader must serve reads");
        assertEquals(1, servedByLeader.getMessages().length);
        assertEquals("only-the-leader-answers-reads", new String(servedByLeader.getMessages()[0],
                StandardCharsets.UTF_8));

        // The follower holds exactly the same bytes on disk and still must not answer.
        // A lagging follower's short log looks identical to an empty partition from the
        // outside, so a consumer could never tell a hole from "nothing here yet".
        Protocol.FetchResult servedByFollower = fetchFrom(portFor(follower), topic, 0, 0, 4096);
        assertFalse(servedByFollower.isSuccess(), "a follower must not serve reads");
        assertTrue(servedByFollower.getError().contains("leader"),
                "the refusal should name the leader, but was: " + servedByFollower.getError());
    }

    @Test
    void aMessageTooLargeForOneReadSurvivesTheRoundTrip() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 2);

        // Comfortably larger than the 1024-byte buffer the broker once used to hold a
        // whole request, so this cannot pass unless the frame is reassembled from its
        // own header - and it is fetched back to prove the reply is framed too.
        byte[] payload = new byte[64 * 1024];
        ThreadLocalRandom.current().nextBytes(payload);

        assertEquals(0, client.send(topic, 0, payload));

        List<byte[]> fetched = client.fetch(topic, 0, 0, payload.length + 4096);
        assertEquals(1, fetched.size(), "the broker must hand back the whole message");
        assertArrayEquals(payload, fetched.get(0), "the message must come back byte for byte");
    }

    /**
     * Fetch from one specific broker, so a test can tell exactly which broker answered
     * instead of whichever one it happened to reach.
     */
    private static Protocol.FetchResult fetchFrom(int port, String topic, int partition, long offset, int maxBytes)
            throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", port));
            Protocol.writeFully(channel, Protocol.encodeFetchRequest(topic, partition, offset, maxBytes));
            return Protocol.readFetchResponse(channel, 30_000);
        }
    }

    @Test
    void produceIsNotAcknowledgedUntilEveryFollowerHasTheMessage() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 2);

        int leader = leaderOf(topic, 0);
        int follower = other(leader);

        for (int i = 0; i < 10; i++) {
            assertEquals(i, client.send(topic, 0, bytes("msg-" + i)));

            // No polling here: the reply itself has to mean the follower already
            // stored the message, otherwise a follower that dies right after the
            // acknowledgement loses data the producer was told is safe.
            SortedMap<Long, String> followerLog = readLog(follower, topic, 0);
            assertEquals(i + 1, followerLog.size(),
                    "produce acknowledged message " + i + " before the follower had it");
            assertEquals("msg-" + i, followerLog.get((long) i));
        }

        assertEquals(readLog(leader, topic, 0), readLog(follower, topic, 0));
    }

    @Test
    void produceIsRejectedWhileAFollowerIsUnreachable() throws Exception {
        String topic = newTopic();
        createTopic(topic, 1, (short) 2);

        int leader = leaderOf(topic, 0);
        int follower = other(leader);
        assertEquals(0, client.send(topic, 0, bytes("before")));

        // Take the follower away, then advertise it again on a port nobody listens
        // on. The leader still counts it as a replica, so every produce from here on
        // has to be refused instead of being acked as if the message were durable on
        // both brokers. A dead port is used rather than just stopping the follower so
        // the leader cannot quietly drop it from its metadata first.
        stopBroker(follower);
        int deadPort = freePort();
        String followerPath = "/brokers/" + follower;

        try {
            try (ZooKeeper lookalike = connectAsTestClient()) {
                lookalike.create(followerPath, ("127.0.0.1:" + deadPort).getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                awaitTrue("the leader to advertise the unreachable follower", TIMEOUT, () ->
                        brokersAt(portFor(leader)).stream()
                                .anyMatch(broker -> broker.getId() == follower
                                        && broker.getPort() == deadPort));

                assertThrows(IOException.class, () -> client.send(topic, 0, bytes("never-replicated")),
                        "a produce must not succeed while a replica cannot be reached");

                // The leader keeps its own copy - Kafka does not roll back either. The
                // client is only told the write is not durable and may retry, which can
                // duplicate the message; what must not happen is a silent success.
                assertTrue(readLog(leader, topic, 0).containsKey(1L),
                        "the leader should have appended before trying to replicate");

                lookalike.delete(followerPath, -1);
            }
        } finally {
            // Leave the cluster as we found it for the rest of the class, whatever the
            // assertions above decided. Skipping this would leave the other tests in
            // this class waiting on a broker that never comes back.
            if (!brokers.containsKey(follower)) {
                startBroker(follower);
            }
            client = new SimpleKafkaClient("127.0.0.1", portFor(BROKER_IDS[0]));
            client.initialize();
        }

        awaitTrue("the follower to rejoin", TIMEOUT, () -> {
            List<Integer> ids = brokerIdsAt(portFor(leader));
            return ids.size() == BROKER_IDS.length && ids.contains(follower);
        });
    }

    // ------------------------------------------------------------------ helpers

    private static void produceAndAwaitReplication(String topic, int count) throws Exception {
        int leader = leaderOf(topic, 0);
        int follower = other(leader);

        for (int i = 0; i < count; i++) {
            assertEquals(i, client.send(topic, 0, bytes("msg-" + i)));
        }

        awaitTrue("replication of " + count + " messages", TIMEOUT, () -> {
            SortedMap<Long, String> leaderLog = readLog(leader, topic, 0);
            SortedMap<Long, String> followerLog = readLog(follower, topic, 0);
            return leaderLog.size() == count && followerLog.equals(leaderLog);
        });
    }

    private static String newTopic() {
        return "repl-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static void createTopic(String topic, int partitions, short replicationFactor) throws Exception {
        awaitTrue("creation of topic " + topic, TIMEOUT, () -> {
            client.refreshMetadata();
            if (client.getTopicMetadata(topic) != null) {
                return true;
            }
            client.createTopic(topic, partitions, replicationFactor);
            client.refreshMetadata();
            return client.getTopicMetadata(topic) != null;
        });
    }

    private static int leaderOf(String topic, int partition) throws IOException {
        for (int brokerId : BROKER_IDS) {
            try {
                for (Protocol.TopicMetadata topicMetadata : fetchMetadata(portFor(brokerId)).getTopics()) {
                    if (!topicMetadata.getName().equals(topic)) {
                        continue;
                    }
                    for (Protocol.PartitionMetadata partitionMetadata : topicMetadata.getPartitions()) {
                        if (partitionMetadata.getId() == partition) {
                            return partitionMetadata.getLeader();
                        }
                    }
                }
            } catch (IOException | RuntimeException e) {
                // try the next broker
            }
        }
        fail("No broker knows about partition " + partition + " of topic " + topic);
        return -1;
    }

    private static int other(int brokerId) {
        return brokerId == BROKER_IDS[0] ? BROKER_IDS[1] : BROKER_IDS[0];
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

    private static byte[] sendReplicate(int port, String topic, int partition, long offset, byte[] message)
            throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", port));
            Protocol.writeFully(channel, Protocol.encodeReplicateRequest(topic, partition, offset, message));

            ByteBuffer response = ByteBuffer.allocate(Protocol.REPLICATION_RESPONSE_SIZE);
            Protocol.readFully(channel, response, 30_000);
            response.flip();

            byte[] reply = new byte[response.remaining()];
            response.get(reply);
            return reply;
        }
    }

    private static long logEndOffset(byte[] reply) {
        return ByteBuffer.wrap(reply, 1, 8).getLong();
    }

    /**
     * Read every segment of a partition straight off disk, so the assertion is about
     * what the broker actually stored rather than what it claims through the API.
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

    private static Protocol.MetadataResult fetchMetadata(int port) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", port));
            Protocol.writeFully(channel, Protocol.encodeMetadataRequest());
            return Protocol.decodeMetadataResponse(readUntilQuiet(channel));
        }
    }

    private static List<Integer> brokerIdsAt(int port) throws IOException {
        List<Integer> ids = new ArrayList<>();
        for (BrokerInfo broker : brokersAt(port)) {
            ids.add(broker.getId());
        }
        return ids;
    }

    private static List<BrokerInfo> brokersAt(int port) throws IOException {
        Protocol.MetadataResult result = fetchMetadata(port);
        if (!result.isSuccess()) {
            throw new IOException("Metadata request to port " + port + " failed: " + result.getError());
        }
        return result.getBrokers();
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
            throw new IllegalStateException("Broker " + brokerId + " is not running");
        }
        return port;
    }

    private static void awaitTrue(String what, long timeoutMillis, Condition condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastFailure = null;

        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.isMet()) {
                    return;
                }
                lastFailure = null;
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(100);
        }

        if (lastFailure != null) {
            throw new AssertionError("Timed out waiting for " + what, lastFailure);
        }
        fail("Timed out waiting for " + what);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A ZooKeeper client of our own, so a test can change what the brokers see about
     * each other instead of waiting for the real thing to happen.
     */
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
        if (!file.delete()) {
            file.deleteOnExit();
        }
    }
}
