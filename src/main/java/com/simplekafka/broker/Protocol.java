package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
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
     * Send an error response to the client
     */
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(3 + errorMessage.length());
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorMessage.length());
        buffer.put(errorMessage.getBytes());
        buffer.flip();
        channel.write(buffer);
    }
    
    /**
     * Encode a producer request
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(11 + topic.length() + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
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
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length());
        buffer.put(FETCH);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
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
        ByteBuffer buffer = ByteBuffer.allocate(9 + topic.length());
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
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
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length() + message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
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
        ByteBuffer buffer = ByteBuffer.allocate(3 + topic.length());
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
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
            channel.configureBlocking(wasBlocking);
        }
    }

    /**
     * Write the whole buffer, looping over partial writes
     */
    public static void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        long deadline = System.currentTimeMillis() + DEFAULT_IO_TIMEOUT_MS;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer);
            if (written == 0) {
                // Nothing could be handed to the socket: back off instead of spinning.
                if (System.currentTimeMillis() > deadline) {
                    throw new IOException("Timed out writing " + buffer.remaining() + " bytes");
                }
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while writing to channel", e);
                }
            }
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
     * Decode a produce response
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new ProduceResult(-1, error);
            }
            return new ProduceResult(-1, "Invalid response type");
        }
        
        long offset = buffer.getLong();
        byte status = buffer.get();
        
        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }
    
    /**
     * Decode a fetch response
     */
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }
        
        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];
        
        for (int i = 0; i < messageCount; i++) {
            long offset = buffer.getLong(); // Skip offset
            int messageSize = buffer.getInt();
            messages[i] = new byte[messageSize];
            buffer.get(messages[i]);
        }
        
        return new FetchResult(messages, null);
    }
    
    /**
     * Decode metadata response
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != METADATA_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(), error);
            }
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }
        
        // Parse broker info
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        
        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();
            short hostLength = buffer.getShort();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes);
            int port = buffer.getInt();
            
            brokers.add(new BrokerInfo(brokerId, host, port));
        }
        
        // Parse topic metadata
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        
        for (int i = 0; i < topicCount; i++) {
            short topicLength = buffer.getShort();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topicName = new String(topicBytes);
            
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
        private final byte[][] messages;
        private final String error;
        
        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
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