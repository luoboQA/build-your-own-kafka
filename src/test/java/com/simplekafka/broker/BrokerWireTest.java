package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.simplekafka.client.SimpleKafkaClient;

/**
 * What a single client connection may put on the wire.
 *
 * <p>The broker reassembles requests in one accumulation buffer and hands each
 * finished frame to the parser as a view of that same buffer. A client is free to
 * put more than one request in a single write, so the buffer can hold a second
 * request while the first one is being parsed - and the frame is only a view, not
 * a copy.
 */
class BrokerWireTest {

    private static final int BROKER_ID = 1;
    private static final long TIMEOUT = 60_000;

    @TempDir
    static Path tempDir;

    private static final List<SimpleKafkaBroker> brokers = new ArrayList<>();
    private static NIOServerCnxnFactory zooKeeperFactory;
    private static ZooKeeperServer zooKeeperServer;
    private static int zooKeeperPort;
    private static int brokerPort;
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

        brokerPort = freePort();
        SimpleKafkaBroker broker = new SimpleKafkaBroker(BROKER_ID, "127.0.0.1", brokerPort, zooKeeperPort);
        broker.start();
        brokers.add(broker);

        client = new SimpleKafkaClient("127.0.0.1", brokerPort);
        client.initialize();
    }

    @AfterAll
    static void stopCluster() throws Exception {
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

        File[] topics = new File("data/" + BROKER_ID).listFiles((dir, name) -> name.startsWith("wire-"));
        if (topics != null) {
            for (File topic : topics) {
                deleteRecursively(topic);
            }
        }
    }

    /**
     * The frame sitting at the front of the buffer must be parsed before the buffer
     * is compacted. Compaction moves the request behind it to the front of the same
     * array, so parsing afterwards reads the wrong bytes.
     *
     * <p>Two requests of different types make the mis-dispatch visible: the produce
     * is answered with a metadata response instead of a produce response.
     */
    @Test
    void twoRequestsInOneWriteAreBothParsed() throws Exception {
        String topic = newTopic();
        createTopic(topic);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", brokerPort));

            // One write, so the broker's first read() takes both requests.
            ByteBuffer produce = Protocol.encodeProduceRequest(topic, 0, bytes("one"));
            ByteBuffer metadata = Protocol.encodeMetadataRequest();
            ByteBuffer both = ByteBuffer.allocate(produce.remaining() + metadata.remaining());
            both.put(produce).put(metadata).flip();
            Protocol.writeFully(channel, both);

            byte type = readTypeByte(channel);
            assertEquals(Protocol.PRODUCE_RESPONSE, type,
                    "the first of two requests in one write must still be dispatched as the "
                            + "produce it is; a " + typeName(type) + " means the frame was "
                            + "compacted before it was parsed");

            ByteBuffer rest = ByteBuffer.allocate(9);
            Protocol.readFully(channel, rest, TIMEOUT);
            rest.flip();
            assertEquals(0, rest.getLong(), "the first produce must land at offset 0");
            assertEquals(0, rest.get(), "the first produce must be acknowledged as a success");
        }

        assertEquals(List.of("one"), messagesIn(topic, 0),
                "the message that was actually stored must be the one that was sent");
    }

    /**
     * The same defect with two requests of the same type, which is the case a
     * pipelining producer hits. Nothing here fails loudly: the second request's bytes
     * are parsed in the first request's place and the broker acknowledges a message
     * the client never sent at that position.
     */
    @Test
    void pipelinedProducesKeepTheirOwnPayloads() throws Exception {
        String topic = newTopic();
        createTopic(topic);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", brokerPort));

            ByteBuffer first = Protocol.encodeProduceRequest(topic, 0, bytes("one"));
            ByteBuffer second = Protocol.encodeProduceRequest(topic, 0, bytes("two"));
            ByteBuffer both = ByteBuffer.allocate(first.remaining() + second.remaining());
            both.put(first).put(second).flip();
            Protocol.writeFully(channel, both);

            assertEquals(0, readProduceOffset(channel), "the first produce must land at offset 0");
            assertEquals(1, readProduceOffset(channel), "the second produce must land at offset 1");
        }

        assertEquals(List.of("one", "two"), messagesIn(topic, 0),
                "each pipelined produce must store its own message, in the order it was sent");
    }

    /**
     * The controller creates a ZooKeeper node and a Partition object per partition, so
     * a count that is merely positive is not a bound at all: one small request could
     * have it allocate until the broker dies.
     */
    @Test
    void anAbsurdPartitionCountIsRefusedRatherThanAttempted() throws Exception {
        String topic = newTopic();
        // Over the limit, but small enough that the unguarded version would still
        // finish - so the test measures the refusal, not a timeout.
        int tooMany = SimpleKafkaBroker.MAX_PARTITIONS_PER_TOPIC + 1000;

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("127.0.0.1", brokerPort));
            Protocol.writeFully(channel,
                    Protocol.encodeCreateTopicRequest(topic, tooMany, (short) 1));

            assertEquals(Protocol.ERROR_RESPONSE, readTypeByte(channel),
                    "a request for " + tooMany + " partitions must be refused");
        }

        assertFalse(new File("data/" + BROKER_ID + "/" + topic).exists(),
                "a refused topic must not leave anything behind");
    }

    @Test
    void topicConfigurationIsChecked() {
        assertNull(SimpleKafkaBroker.validateTopicConfig(3, (short) 2, 3));

        assertNotNull(SimpleKafkaBroker.validateTopicConfig(0, (short) 1, 3));
        assertNotNull(SimpleKafkaBroker.validateTopicConfig(-1, (short) 1, 3));
        assertNotNull(SimpleKafkaBroker.validateTopicConfig(
                SimpleKafkaBroker.MAX_PARTITIONS_PER_TOPIC + 1, (short) 1, 3));
        assertNotNull(SimpleKafkaBroker.validateTopicConfig(3, (short) 0, 3));
        assertNotNull(SimpleKafkaBroker.validateTopicConfig(3, (short) 4, 3),
                "a replication factor the cluster cannot satisfy must be refused");
    }

    @Test
    void aFetchBudgetIsClampedToSomethingAMachineCanHold() {
        assertEquals(SimpleKafkaBroker.MAX_FETCH_BYTES,
                SimpleKafkaBroker.sanitizeFetchMaxBytes(Integer.MAX_VALUE));
        assertEquals(SimpleKafkaBroker.MAX_FETCH_BYTES,
                SimpleKafkaBroker.sanitizeFetchMaxBytes(SimpleKafkaBroker.MAX_FETCH_BYTES + 1));
        assertEquals(1024, SimpleKafkaBroker.sanitizeFetchMaxBytes(1024));
        assertEquals(0, SimpleKafkaBroker.sanitizeFetchMaxBytes(-1));
    }

    /**
     * A client that goes away mid-request is a normal end to a connection, not a
     * broker fault. The smoke test treats any SEVERE line in a broker log as a
     * failure, so a consumer exiting while a fetch is in flight must not be able to
     * fail an otherwise healthy cluster.
     */
    @Test
    void aClientThatDisappearsMidRequestIsNotReportedAsABrokerError() throws Exception {
        LogCapture capture = LogCapture.attach();
        try {
            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(new InetSocketAddress("127.0.0.1", brokerPort));
                channel.socket().setSoLinger(true, 0); // so close() resets instead of closing cleanly

                // The request is never completed, so the topic never has to exist.
                ByteBuffer partial = Protocol.encodeProduceRequest("wire-nowhere", 0, bytes("half"));
                partial.limit(partial.limit() - 2); // one request short of complete
                channel.write(partial);

                // The broker now waits for the rest of a request that never arrives.
                Thread.sleep(200);
            } // close() with SO_LINGER 0 makes the peer see a connection reset

            // Wait for the broker to have dealt with the reset one way or the other,
            // then assert on how it dealt with it.
            awaitTrue("the broker to handle the connection reset", 15_000, () ->
                    capture.countContaining("Client connection ended") > 0
                            || capture.count(Level.SEVERE, "Error handling client") > 0);

            assertEquals(0, capture.count(Level.SEVERE, "Error handling client"),
                    "a client that disconnects mid-request must not be logged as a broker error");
        } finally {
            capture.detach();
        }
    }

    private static byte readTypeByte(SocketChannel channel) throws Exception {
        ByteBuffer type = ByteBuffer.allocate(1);
        Protocol.readFully(channel, type, TIMEOUT);
        type.flip();
        return type.get();
    }

    private static long readProduceOffset(SocketChannel channel) throws Exception {
        ByteBuffer response = Protocol.readProduceResponse(channel, TIMEOUT);
        Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);
        assertTrue(result.isSuccess(), "produce failed: " + result.getError());
        return result.getOffset();
    }

    private static String typeName(byte type) {
        if (type == Protocol.METADATA_RESPONSE) {
            return "METADATA_RESPONSE";
        }
        if (type == Protocol.ERROR_RESPONSE) {
            return "ERROR_RESPONSE";
        }
        return "type " + type;
    }

    private static String newTopic() {
        return "wire-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static void createTopic(String topic) throws Exception {
        awaitTrue("creation of topic " + topic, () -> {
            client.refreshMetadata();
            if (client.getTopicMetadata(topic) != null) {
                return true;
            }
            client.createTopic(topic, 1, (short) 1);
            client.refreshMetadata();
            return client.getTopicMetadata(topic) != null;
        });
    }

    /**
     * Read the partition straight off disk, so the assertion is about what the broker
     * actually stored rather than what it acknowledged.
     */
    private static List<String> messagesIn(String topic, int partition) throws Exception {
        File dir = new File("data/" + BROKER_ID + "/" + topic + "/" + partition);
        SortedMap<Long, String> entries = new TreeMap<>();

        File[] segments = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (segments == null) {
            return List.of();
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

        return new ArrayList<>(entries.values());
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

    /**
     * Collects the broker logs so a test can assert on what was reported rather than
     * scraping stdout.
     */
    private static final class LogCapture extends Handler {
        private final List<LogRecord> records = Collections.synchronizedList(new ArrayList<>());

        static LogCapture attach() {
            LogCapture capture = new LogCapture();
            capture.setLevel(Level.ALL);
            Logger.getLogger("com.simplekafka.broker").addHandler(capture);
            return capture;
        }

        void detach() {
            Logger.getLogger("com.simplekafka.broker").removeHandler(this);
        }

        long count(Level level, String messagePrefix) {
            synchronized (records) {
                return records.stream()
                        .filter(record -> level.equals(record.getLevel()))
                        .filter(record -> record.getMessage() != null
                                && record.getMessage().startsWith(messagePrefix))
                        .count();
            }
        }

        long countContaining(String text) {
            synchronized (records) {
                return records.stream()
                        .filter(record -> record.getMessage() != null && record.getMessage().contains(text))
                        .count();
            }
        }

        @Override
        public void publish(LogRecord record) {
            if (record != null) {
                records.add(record);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
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
