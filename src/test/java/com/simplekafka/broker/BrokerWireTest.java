package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.List;
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
        long deadline = System.currentTimeMillis() + TIMEOUT;
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
