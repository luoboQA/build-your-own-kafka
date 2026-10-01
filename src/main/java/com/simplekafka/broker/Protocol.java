package com.simplekafka.broker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the wire protocol for Build Your Own Kafka
 */
public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    
    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;
    
    // Internal broker communication
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;
    /** The follower cannot write at the offset the leader picked (its log is behind). */
    public static final byte REPLICATE_NACK = 0x24;
    /** The follower does not know the topic/partition, or the write failed. */
    public static final byte REPLICATE_FAILED = 0x00;

    /**
     * Replication responses are {@code [1 byte status][8 bytes log end offset]}.
     * The log end offset tells the leader where the follower stopped, so it can
     * re-send everything the follower is missing.
     */
    public static final int REPLICATION_RESPONSE_SIZE = 9;
    /** Upper bound for a single socket write performed by the protocol helpers. */
    public static final long DEFAULT_IO_TIMEOUT_MS = 10_000;
    /**
     * How long a produce may take end to end. The leader waits for its followers to
     * store the message before it answers, so the reply can arrive much later than a
     * local append would take - and the wait has to be bounded somewhere.
     */
    public static final long PRODUCE_RESPONSE_TIMEOUT_MS = 30_000;
    /**
     * Sanity bounds for a fetch reply. Nothing on this wire can legitimately exceed
     * them, so a larger value means the frame is corrupt rather than merely big -
     * and without the bound a bad length would be handed straight to the allocator.
     */
    public static final int MAX_FETCH_RECORDS = 1_000_000;
    public static final int MAX_FETCH_RECORD_BYTES = 16 * 1024 * 1024;
    /**
     * Sanity bound for the counts in a metadata reply. A corrupt one would otherwise
     * decide how many things are read from the socket next.
     */
    public static final int MAX_METADATA_ENTRIES = 1_000_000;
    
    /**
     * The UTF-8 bytes of a string.
     *
     * <p>Every length on this wire counts <em>bytes</em>, and so does every buffer
     * size. {@code String.length()} counts UTF-16 code units, which agree with the
     * byte count only for ASCII - a topic called "café" is four chars but five
     * bytes, and sizing a buffer or writing a length prefix from the char count
     * would overflow the buffer and describe the wrong span to the reader.
     */
    public static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Read a 2-byte length field as the unsigned value the wire defines it to be.
     * {@code getShort()} sign-extends, so any length of 32768 or more would arrive
     * as a negative number and be handed straight to an array allocation.
     */
    private static int unsignedShort(ByteBuffer buffer) {
        return buffer.getShort() & 0xFFFF;
    }

    /**
     * Send an error response to the client
     */
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        byte[] text = utf8(errorMessage);
        // The length field is 2 bytes, so the message has to fit in one.
        if (text.length > 0xFFFF) {
            text = java.util.Arrays.copyOf(text, 0xFFFF);
        }

        ByteBuffer buffer = ByteBuffer.allocate(3 + text.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) text.length);
        buffer.put(text);
        buffer.flip();
        writeFully(channel, buffer);
    }

    /**
     * Encode a producer request
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = utf8(topic);
        ByteBuffer buffer = ByteBuffer.allocate(11 + topicBytes.length + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }
    
    /**
     * Encode a fetch request
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = utf8(topic);
        ByteBuffer buffer = ByteBuffer.allocate(19 + topicBytes.length);
        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }
    
    /**
     * Encode a metadata request
     */
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }
    
    /**
     * Encode a create topic request
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = utf8(topic);
        ByteBuffer buffer = ByteBuffer.allocate(9 + topicBytes.length);
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }
    
    /**
     * Encode a replication request.
     *
     * <p>{@code offset} is the position the <em>leader</em> assigned to the message;
     * the follower must write it there rather than at its own log end offset.
     */
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        // Header: 1 (type) + 2 (topic length) + 4 (partition) + 8 (offset) + 4 (message length) = 19
        byte[] topicBytes = utf8(topic);
        ByteBuffer buffer = ByteBuffer.allocate(19 + topicBytes.length + message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }
    
    /**
     * Encode a topic notification
     */
    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = utf8(topic);
        ByteBuffer buffer = ByteBuffer.allocate(3 + topicBytes.length);
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a replication response: status plus the follower's log end offset
     */
    public static ByteBuffer encodeReplicationResponse(byte status, long logEndOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(REPLICATION_RESPONSE_SIZE);
        buffer.put(status);
        buffer.putLong(logEndOffset);
        buffer.flip();
        return buffer;
    }

    /**
     * Read exactly {@code buffer.remaining()} bytes from the channel, looping over
     * partial reads. A single {@code channel.read()} may return fewer bytes than
     * asked for, which would otherwise leave the response half parsed.
     */
    public static void readFully(SocketChannel channel, ByteBuffer buffer, long timeoutMillis) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        boolean wasBlocking = channel.isBlocking();
        channel.configureBlocking(false);
        try {
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer);
                if (read < 0) {
                    throw new IOException("Connection closed after " + buffer.position() +
                            " of " + buffer.limit() + " bytes");
                }
                if (read == 0) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new IOException("Timed out after " + timeoutMillis + "ms with " +
                                buffer.remaining() + " bytes still missing");
                    }
                    Thread.sleep(2);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading from channel", e);
        } finally {
            restoreBlockingMode(channel, wasBlocking);
        }
    }

    /**
     * Write the whole buffer, looping over partial writes
     */
    public static void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        writeFully(channel, buffer, DEFAULT_IO_TIMEOUT_MS);
    }

    /**
     * Write the whole buffer within a deadline.
     *
     * <p>The channel is switched to non-blocking for the duration, for the same
     * reason {@link #readFully} does it: on a blocking channel {@code write} does not
     * return 0 when the socket buffer fills, it blocks inside the socket - so the
     * deadline below would never be consulted and a peer that stopped reading would
     * hang this thread for as long as the operating system allowed.
     */
    public static void writeFully(SocketChannel channel, ByteBuffer buffer, long timeoutMillis)
            throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        boolean wasBlocking = channel.isBlocking();
        channel.configureBlocking(false);
        try {
            while (buffer.hasRemaining()) {
                int written = channel.write(buffer);
                if (written == 0) {
                    // Nothing could be handed to the socket: back off instead of spinning.
                    if (System.currentTimeMillis() > deadline) {
                        throw new IOException("Timed out writing " + buffer.remaining() + " bytes");
                    }
                    Thread.sleep(2);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing to channel", e);
        } finally {
            restoreBlockingMode(channel, wasBlocking);
        }
    }

    /**
     * Put the channel back the way it was found.
     *
     * <p>A channel that has already been closed cannot be reconfigured, and that
     * failure must not replace the error that caused the close.
     */
    private static void restoreBlockingMode(SocketChannel channel, boolean wasBlocking) {
        try {
            channel.configureBlocking(wasBlocking);
        } catch (IOException ignored) {
            // The channel is gone; the caller is already unwinding a failure.
        }
    }
    
    /**
     * Read exactly one produce response frame.
     *
     * <p>Produce replies are either the fixed 10 byte success frame or an error frame
     * that carries its own length, so the frame is read by its shape instead of
     * hoping one {@code read()} returns all of it. The leader now waits for its
     * followers before answering, which makes a late or split reply much more likely
     * than it was when produce returned right after the local append.
     *
     * @return the complete frame, ready for {@link #decodeProduceResponse}
     */
    public static ByteBuffer readProduceResponse(SocketChannel channel, long timeoutMillis) throws IOException {
        ByteBuffer type = ByteBuffer.allocate(1);
        readFully(channel, type, timeoutMillis);
        type.flip();
        byte responseType = type.get();

        int frameLength;
        if (responseType == PRODUCE_RESPONSE) {
            frameLength = 9; // offset + status
        } else if (responseType == ERROR_RESPONSE) {
            ByteBuffer length = ByteBuffer.allocate(2);
            readFully(channel, length, timeoutMillis);
            length.flip();
            frameLength = 2 + (length.getShort() & 0xFFFF);
        } else {
            throw new IOException("Unexpected produce response type: " + responseType);
        }

        ByteBuffer rest = ByteBuffer.allocate(frameLength);
        readFully(channel, rest, timeoutMillis);
        rest.flip();

        ByteBuffer frame = ByteBuffer.allocate(1 + frameLength);
        frame.put(responseType);
        frame.put(rest);
        frame.flip();
        return frame;
    }

    /**
     * Read exactly one fetch response.
     *
     * <p>Same reason as {@link #readProduceResponse}: there is no length prefix for
     * the whole reply and one {@code read()} may return a fragment, so the reader has
     * to follow the frame's own structure - count, then each record's length - rather
     * than stop when the socket goes quiet. That matters far more here than for a
     * produce reply, because a fetch answer is the one place where a large message is
     * actually sent back to the caller.
     *
     * @return the decoded reply, either the records or the broker's error text
     */
    public static FetchResult readFetchResponse(SocketChannel channel, long timeoutMillis) throws IOException {
        ByteBuffer type = ByteBuffer.allocate(1);
        readFully(channel, type, timeoutMillis);
        type.flip();
        byte responseType = type.get();

        if (responseType == ERROR_RESPONSE) {
            return new FetchResult(new long[0], new byte[0][], readErrorText(channel, timeoutMillis));
        }
        if (responseType != FETCH_RESPONSE) {
            throw new IOException("Unexpected fetch response type: " + responseType);
        }

        ByteBuffer count = ByteBuffer.allocate(4);
        readFully(channel, count, timeoutMillis);
        count.flip();
        int messageCount = count.getInt();
        if (messageCount < 0 || messageCount > MAX_FETCH_RECORDS) {
            throw new IOException("Implausible fetch response with " + messageCount + " records");
        }

        long[] offsets = new long[messageCount];
        byte[][] messages = new byte[messageCount][];
        for (int i = 0; i < messageCount; i++) {
            ByteBuffer header = ByteBuffer.allocate(12); // offset + record length
            readFully(channel, header, timeoutMillis);
            header.flip();
            // The offset is kept rather than assumed. A reader that could not start
            // exactly where it was asked to - the index fallback allows that - would
            // otherwise have its records silently labelled with offsets they do not
            // occupy, and nothing downstream could tell.
            offsets[i] = header.getLong();
            int size = header.getInt();
            if (size < 0 || size > MAX_FETCH_RECORD_BYTES) {
                throw new IOException("Implausible record size: " + size);
            }

            ByteBuffer payload = ByteBuffer.allocate(size);
            readFully(channel, payload, timeoutMillis);
            payload.flip();
            messages[i] = new byte[size];
            payload.get(messages[i]);
        }

        return new FetchResult(offsets, messages, null);
    }

    /**
     * Read the text of an {@link #ERROR_RESPONSE} that has already had its type byte
     * consumed. Its length is a unsigned short, so the payload is bounded at 64KB and
     * needs no further policing.
     */
    private static String readErrorText(SocketChannel channel, long timeoutMillis) throws IOException {
        ByteBuffer length = ByteBuffer.allocate(2);
        readFully(channel, length, timeoutMillis);
        length.flip();
        int errorLength = length.getShort() & 0xFFFF;

        ByteBuffer text = ByteBuffer.allocate(errorLength);
        readFully(channel, text, timeoutMillis);
        text.flip();
        byte[] bytes = new byte[errorLength];
        text.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Decode a produce response
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                byte[] errorBytes = new byte[unsignedShort(buffer)];
                buffer.get(errorBytes);
                return new ProduceResult(-1, new String(errorBytes, StandardCharsets.UTF_8));
            }
            return new ProduceResult(-1, "Invalid response type");
        }
        
        long offset = buffer.getLong();
        byte status = buffer.get();
        
        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }
    
    /**
     * Read exactly one metadata response.
     *
     * <p>This is the one reply whose size grows with the cluster - a few dozen bytes per
     * broker and per partition - and it carries no length prefix, so it has to be walked
     * by its own structure: counts first, each name's length before its bytes. Reading it
     * into a fixed buffer works until the cluster has enough topics to overflow one, and
     * then the decode fails on a truncated frame, leaving a client that cannot find any
     * broker at all.
     */
    public static ByteBuffer readMetadataResponse(SocketChannel channel, long timeoutMillis)
            throws IOException {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();

        byte[] type = new byte[1];
        readExactly(channel, type, timeoutMillis);
        frame.write(type, 0, 1);

        if (type[0] == ERROR_RESPONSE) {
            byte[] length = new byte[2];
            readExactly(channel, length, timeoutMillis);
            frame.write(length, 0, 2);
            int errorLength = ((length[0] & 0xFF) << 8) | (length[1] & 0xFF);
            readExactly(channel, frame, errorLength, timeoutMillis);
            return ByteBuffer.wrap(frame.toByteArray());
        }

        if (type[0] != METADATA_RESPONSE) {
            throw new IOException("Unexpected metadata response type: " + type[0]);
        }

        int brokerCount = readCount(channel, frame, timeoutMillis, "broker");
        for (int i = 0; i < brokerCount; i++) {
            readRawInt(channel, frame, timeoutMillis); // id
            int hostLength = readShort(channel, frame, timeoutMillis);
            readExactly(channel, frame, hostLength, timeoutMillis);
            readRawInt(channel, frame, timeoutMillis); // port
        }

        int topicCount = readCount(channel, frame, timeoutMillis, "topic");
        for (int i = 0; i < topicCount; i++) {
            int nameLength = readShort(channel, frame, timeoutMillis);
            readExactly(channel, frame, nameLength, timeoutMillis);

            int partitionCount = readCount(channel, frame, timeoutMillis, "partition");
            for (int j = 0; j < partitionCount; j++) {
                readRawInt(channel, frame, timeoutMillis); // partition id
                readRawInt(channel, frame, timeoutMillis); // leader, which may be -1

                int replicaCount = readCount(channel, frame, timeoutMillis, "replica");
                for (int k = 0; k < replicaCount; k++) {
                    readRawInt(channel, frame, timeoutMillis);
                }
            }
        }

        return ByteBuffer.wrap(frame.toByteArray());
    }

    /**
     * Read a count and refuse an implausible one. A count off the wire decides how many
     * things are read next, so a corrupt one would otherwise have this reading for ever.
     */
    private static int readCount(SocketChannel channel, ByteArrayOutputStream frame, long timeoutMillis, String what)
            throws IOException {
        int value = readRawInt(channel, frame, timeoutMillis);
        if (value < 0 || value > MAX_METADATA_ENTRIES) {
            throw new IOException("Implausible " + what + " count: " + value);
        }
        return value;
    }

    private static int readRawInt(SocketChannel channel, ByteArrayOutputStream frame, long timeoutMillis)
            throws IOException {
        byte[] bytes = new byte[4];
        readExactly(channel, bytes, timeoutMillis);
        frame.write(bytes, 0, 4);
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16)
                | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    private static int readShort(SocketChannel channel, ByteArrayOutputStream frame, long timeoutMillis)
            throws IOException {
        byte[] bytes = new byte[2];
        readExactly(channel, bytes, timeoutMillis);
        frame.write(bytes, 0, 2);
        return ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
    }

    private static void readExactly(SocketChannel channel, byte[] target, long timeoutMillis) throws IOException {
        readFully(channel, ByteBuffer.wrap(target), timeoutMillis);
    }

    private static void readExactly(SocketChannel channel, ByteArrayOutputStream frame, int length, long timeoutMillis)
            throws IOException {
        byte[] bytes = new byte[length];
        readFully(channel, ByteBuffer.wrap(bytes), timeoutMillis);
        frame.write(bytes, 0, length);
    }

    /**
     * Decode metadata response
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != METADATA_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                byte[] errorBytes = new byte[unsignedShort(buffer)];
                buffer.get(errorBytes);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(),
                        new String(errorBytes, StandardCharsets.UTF_8));
            }
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }
        
        // Parse broker info
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        
        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();
            byte[] hostBytes = new byte[unsignedShort(buffer)];
            buffer.get(hostBytes);
            String host = new String(hostBytes, StandardCharsets.UTF_8);
            int port = buffer.getInt();
            
            brokers.add(new BrokerInfo(brokerId, host, port));
        }
        
        // Parse topic metadata
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        
        for (int i = 0; i < topicCount; i++) {
            byte[] topicBytes = new byte[unsignedShort(buffer)];
            buffer.get(topicBytes);
            String topicName = new String(topicBytes, StandardCharsets.UTF_8);
            
            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();
            
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();
                
                int replicas = buffer.getInt();
                List<Integer> replicaIds = new ArrayList<>();
                
                for (int k = 0; k < replicas; k++) {
                    replicaIds.add(buffer.getInt());
                }
                
                partitions.add(new PartitionMetadata(partitionId, leaderId, replicaIds));
            }
            
            topics.add(new TopicMetadata(topicName, partitions));
        }
        
        return new MetadataResult(brokers, topics, null);
    }
    
    /**
     * Result class for produce operations
     */
    public static class ProduceResult {
        private final long offset;
        private final String error;
        
        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }
        
        public long getOffset() {
            return offset;
        }
        
        public String getError() {
            return error;
        }
        
        public boolean isSuccess() {
            return error == null;
        }
    }
    
    /**
     * Result class for fetch operations
     */
    public static class FetchResult {
        private final long[] offsets;
        private final byte[][] messages;
        private final String error;

        public FetchResult(long[] offsets, byte[][] messages, String error) {
            this.offsets = offsets;
            this.messages = messages;
            this.error = error;
        }

        /**
         * The offset each message occupies, position for position with
         * {@link #getMessages()}.
         */
        public long[] getOffsets() {
            return offsets;
        }

        public byte[][] getMessages() {
            return messages;
        }
        
        public int getMessageCount() {
            return messages.length;
        }
        
        public String getError() {
            return error;
        }
        
        public boolean isSuccess() {
            return error == null;
        }
    }
    
    /**
     * A message together with the offset it occupies in its partition.
     *
     * <p>The two only mean anything together: an offset assumed from the one that was
     * asked for is wrong the moment a reader starts anywhere else, and nothing
     * downstream can tell the difference.
     */
    public static class Record {
        private final long offset;
        private final byte[] payload;

        public Record(long offset, byte[] payload) {
            this.offset = offset;
            this.payload = payload;
        }

        public long getOffset() {
            return offset;
        }

        public byte[] getPayload() {
            return payload;
        }
    }

    /**
     * Result class for metadata operations
     */
    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;
        
        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }
        
        public List<BrokerInfo> getBrokers() {
            return brokers;
        }
        
        public List<TopicMetadata> getTopics() {
            return topics;
        }
        
        public String getError() {
            return error;
        }
        
        public boolean isSuccess() {
            return error == null;
        }
    }
    
    /**
     * Topic metadata class
     */
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;
        
        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
        
        public String getName() {
            return name;
        }
        
        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }
    
    /**
     * Partition metadata class
     */
    public static class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;
        
        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }
        
        public int getId() {
            return id;
        }
        
        public int getLeader() {
            return leader;
        }
        
        public List<Integer> getReplicas() {
            return replicas;
        }
    }
}