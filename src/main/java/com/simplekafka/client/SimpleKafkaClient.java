package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for interacting with Build Your Own Kafka brokers
 */
public class SimpleKafkaClient {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());
    
    private final String bootstrapBroker;
    private final int bootstrapPort;
    /**
     * Both maps are replaced wholesale on every refresh rather than cleared and refilled,
     * so a reader on another thread never finds one of them empty in between - which used
     * to show up as "Topic not found" for a topic that plainly existed.
     */
    private volatile Map<String, TopicMetadata> topicMetadata;
    private volatile Map<Integer, BrokerInfo> brokers;
    private final AtomicInteger correlationId;
    
    /**
     * Create a SimpleKafka client
     * @param bootstrapBroker Host of a broker to connect to
     * @param bootstrapPort Port of the broker to connect to
     */
    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
        this.brokers = new ConcurrentHashMap<>();
        this.correlationId = new AtomicInteger(0);
    }
    
    /**
     * Initialize the client by fetching cluster metadata
     */
    public void initialize() throws IOException {
        refreshMetadata();
    }
    
    public void refreshMetadata() throws IOException {
        List<InetSocketAddress> candidates = new ArrayList<>();
        candidates.add(new InetSocketAddress(bootstrapBroker, bootstrapPort));
        for (BrokerInfo broker : brokers.values()) {
            InetSocketAddress address = new InetSocketAddress(broker.getHost(), broker.getPort());
            if (!candidates.contains(address)) {
                candidates.add(address);
            }
        }

        // The bootstrap address is tried first, then every broker already known. Insisting
        // on the bootstrap alone makes it a single point of failure for a client that has a
        // list of live brokers: the cluster can be perfectly healthy and the client still
        // unable to refresh, because the one address it will talk to has gone.
        IOException lastFailure = null;
        for (InetSocketAddress address : candidates) {
            try {
                publish(readMetadataFrom(address));
                return;
            } catch (IOException | RuntimeException e) {
                lastFailure = new IOException("Metadata request to " + address + " failed", e);
            }
        }

        throw lastFailure == null
                ? new IOException("No broker available to ask for metadata")
                : lastFailure;
    }

    private Protocol.MetadataResult readMetadataFrom(InetSocketAddress address) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(address);

            // Request metadata
            Protocol.writeFully(channel, Protocol.encodeMetadataRequest());

            // Read response. This is the one reply with no fixed size: it grows with the
            // cluster, so it is walked by its own structure rather than read into a buffer
            // that a large enough cluster would overflow.
            ByteBuffer response = Protocol.readMetadataResponse(channel, Protocol.DEFAULT_IO_TIMEOUT_MS);

            // Parse response
            return Protocol.decodeMetadataResponse(response);
        }
    }

    private void publish(Protocol.MetadataResult result) throws IOException {
        if (!result.isSuccess()) {
            throw new IOException("Failed to fetch metadata: " + result.getError());
        }

        // Built into fresh maps and published by replacing the references, so a reader on
        // another thread never finds one half-filled.
        Map<Integer, BrokerInfo> freshBrokers = new ConcurrentHashMap<>();
        for (BrokerInfo broker : result.getBrokers()) {
            freshBrokers.put(broker.getId(), new BrokerInfo(broker.getId(), broker.getHost(), broker.getPort()));
        }

        Map<String, TopicMetadata> freshTopics = new ConcurrentHashMap<>();
        for (Protocol.TopicMetadata topic : result.getTopics()) {
            List<PartitionInfo> partitions = new ArrayList<>();
            for (Protocol.PartitionMetadata partition : topic.getPartitions()) {
                partitions.add(new PartitionInfo(
                    partition.getId(),
                    partition.getLeader(),
                    partition.getReplicas()
                ));
            }
            freshTopics.put(topic.getName(), new TopicMetadata(topic.getName(), partitions));
        }

        this.brokers = freshBrokers;
        this.topicMetadata = freshTopics;

        LOGGER.info("Metadata refreshed: " + freshBrokers.size() + " brokers, " +
                   freshTopics.size() + " topics");
    }
    
    /**
     * Create a new topic
     */
    public boolean createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        // Find a broker to send the request to (prefer controller if known)
        if (brokers.isEmpty()) {
            refreshMetadata();
            if (brokers.isEmpty()) {
                throw new IOException("No brokers available");
            }
        }
        
        BrokerInfo broker = brokers.values().iterator().next();
        
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));
            
            // Send create topic request
            ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);
            Protocol.writeFully(channel, request);
            
            // Read response. One byte tells the shape - a bare status, or an error frame
            // with its own length - so it is read that way rather than into a fixed buffer
            // that a long enough error message would overflow.
            byte[] typeBytes = new byte[1];
            Protocol.readFully(channel, ByteBuffer.wrap(typeBytes), Protocol.DEFAULT_IO_TIMEOUT_MS);
            byte responseType = typeBytes[0];

            if (responseType != Protocol.CREATE_TOPIC_RESPONSE) {
                if (responseType == Protocol.ERROR_RESPONSE) {
                    byte[] lengthBytes = new byte[2];
                    Protocol.readFully(channel, ByteBuffer.wrap(lengthBytes), Protocol.DEFAULT_IO_TIMEOUT_MS);
                    int errorLength = ((lengthBytes[0] & 0xFF) << 8) | (lengthBytes[1] & 0xFF);

                    byte[] errorBytes = new byte[errorLength];
                    Protocol.readFully(channel, ByteBuffer.wrap(errorBytes), Protocol.DEFAULT_IO_TIMEOUT_MS);
                    String error = new String(errorBytes, StandardCharsets.UTF_8);
                    LOGGER.warning("Error creating topic: " + error);
                    return false;
                }
                throw new IOException("Invalid create topic response type: " + responseType);
            }

            byte[] statusBytes = new byte[1];
            Protocol.readFully(channel, ByteBuffer.wrap(statusBytes), Protocol.DEFAULT_IO_TIMEOUT_MS);
            boolean success = statusBytes[0] == 0;
            
            if (success) {
                // Refresh metadata to include new topic
                refreshMetadata();
            }
            
            return success;
        }
    }
    
    /**
     * Produce a message to a topic-partition
     */
    /**
     * Produce a message to a topic-partition
     */
    public long send(String topic, int partition, byte[] message) throws IOException {
        return send(topic, partition, message, true);
    }

    private long send(String topic, int partition, byte[] message, boolean mayRetry) throws IOException {
        BrokerInfo leader = leaderFor(topic, partition);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            // Send produce request
            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
            Protocol.writeFully(channel, request);

            // Read response. The leader waits for its followers to store the message
            // before it replies, so the answer can arrive well after the request - and it
            // may well arrive in more than one piece.
            ByteBuffer response = Protocol.readProduceResponse(channel, Protocol.PRODUCE_RESPONSE_TIMEOUT_MS);

            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Failed to produce message: " + result.getError());
            }

            return result.getOffset();
        } catch (ConnectException e) {
            // The broker was never reached, so nothing can have been written there. That is
            // what makes this safe to retry, and a timeout is not: a produce that timed out
            // may well have been accepted, and retrying it would duplicate the message.
            if (!mayRetry) {
                throw e;
            }
            LOGGER.warning("Broker " + leader.getId() + " is unreachable; refreshing metadata and retrying");
            refreshMetadata();
            return send(topic, partition, message, false);
        }
    }
    
    /**
     * Consume messages from a topic-partition, without their offsets.
     *
     * <p>Prefer {@link #fetchRecords}: an offset only means something alongside its
     * bytes, and a caller that assumes the first message is the one it asked for is
     * wrong whenever the broker had to start earlier than that.
     */
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        List<byte[]> messages = new ArrayList<>();
        for (Protocol.Record record : fetchRecords(topic, partition, offset, maxBytes)) {
            messages.add(record.getPayload());
        }
        return messages;
    }

    /**
     * Consume records from a topic-partition, each carrying the offset it occupies -
     * which is not necessarily the offset that was asked for.
     */
    public List<Protocol.Record> fetchRecords(String topic, int partition, long offset, int maxBytes)
            throws IOException {
        return fetchRecords(topic, partition, offset, maxBytes, true);
    }

    private List<Protocol.Record> fetchRecords(String topic, int partition, long offset, int maxBytes,
            boolean mayRetry) throws IOException {
        BrokerInfo leader = leaderFor(topic, partition);

        // Fetch from leader
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            // Send fetch request
            Protocol.writeFully(channel, Protocol.encodeFetchRequest(topic, partition, offset, maxBytes));

            // Read response. A fetch answer can be far larger than a socket buffer, so it
            // is walked frame by frame instead of read once and hoped for.
            Protocol.FetchResult result =
                    Protocol.readFetchResponse(channel, Protocol.DEFAULT_IO_TIMEOUT_MS);

            if (!result.isSuccess()) {
                // Only the leader answers reads, so being told this broker is not it means
                // the assignment moved. Refreshing and asking the broker that leads now is
                // the whole recovery: without it a client that cached the old leader could
                // never read that partition again.
                if (mayRetry && result.getError() != null && result.getError().contains("is not the leader")) {
                    LOGGER.warning("Broker " + leader.getId() + " no longer leads " + topic + "/" + partition +
                            "; refreshing metadata and retrying");
                    refreshMetadata();
                    return fetchRecords(topic, partition, offset, maxBytes, false);
                }
                throw new IOException("Failed to fetch messages: " + result.getError());
            }

            long[] offsets = result.getOffsets();
            byte[][] messages = result.getMessages();
            List<Protocol.Record> records = new ArrayList<>();
            for (int i = 0; i < messages.length; i++) {
                records.add(new Protocol.Record(offsets[i], messages[i]));
            }
            return records;
        } catch (ConnectException e) {
            if (!mayRetry) {
                throw e;
            }
            LOGGER.warning("Broker " + leader.getId() + " is unreachable; refreshing metadata and retrying");
            refreshMetadata();
            return fetchRecords(topic, partition, offset, maxBytes, false);
        }
    }

    /**
     * The broker that leads a partition.
     *
     * <p>The partition is looked up again after a refresh rather than reusing the leader id
     * read before it. The whole point of refreshing is that the assignment may have moved,
     * so checking the old id against the new broker list would either find nothing or find
     * the broker that was just replaced.
     */
    private BrokerInfo leaderFor(String topic, int partition) throws IOException {
        PartitionInfo info = partitionInfo(topic, partition);

        BrokerInfo leader = brokers.get(info.getLeader());
        if (leader == null) {
            // No address for the broker the assignment names: refreshing is the only way
            // to learn one, because the assignment came from the same place.
            refreshMetadata();
            info = partitionInfo(topic, partition);
            leader = brokers.get(info.getLeader());
        }

        if (leader == null) {
            throw new IOException("Leader broker " + info.getLeader() + " of " + topic + "/" + partition +
                    " is not in the cluster metadata");
        }
        return leader;
    }

    private PartitionInfo partitionInfo(String topic, int partition) throws IOException {
        if (!topicMetadata.containsKey(topic)) {
            refreshMetadata();
            if (!topicMetadata.containsKey(topic)) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        for (PartitionInfo info : topicMetadata.get(topic).getPartitions()) {
            if (info.getId() == partition) {
                return info;
            }
        }
        throw new IOException("Partition not found: " + partition);
    }
    
    /**
     * Get metadata for all topics
     */
    public Map<String, TopicMetadata> getTopicMetadata() {
        return new HashMap<>(topicMetadata);
    }
    
    /**
     * Get metadata for a specific topic
     */
    public TopicMetadata getTopicMetadata(String topic) {
        return topicMetadata.get(topic);
    }
    
    /**
     * Get broker information
     */
    public Map<Integer, BrokerInfo> getBrokers() {
        return new HashMap<>(brokers);
    }
    
    /**
     * Topic metadata
     */
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionInfo> partitions;
        
        public TopicMetadata(String name, List<PartitionInfo> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
        
        public String getName() {
            return name;
        }
        
        public List<PartitionInfo> getPartitions() {
            return new ArrayList<>(partitions);
        }
        
        @Override
        public String toString() {
            return "TopicMetadata{name='" + name + "', partitions=" + partitions + "}";
        }
    }
    
    /**
     * Partition information
     */
    public static class PartitionInfo {
        private final int id;
        private final int leader;
        private final List<Integer> followers;
        
        public PartitionInfo(int id, int leader, List<Integer> followers) {
            this.id = id;
            this.leader = leader;
            this.followers = followers;
        }
        
        public int getId() {
            return id;
        }
        
        public int getLeader() {
            return leader;
        }
        
        public List<Integer> getFollowers() {
            return new ArrayList<>(followers);
        }
        
        @Override
        public String toString() {
            return "PartitionInfo{id=" + id + ", leader=" + leader + ", followers=" + followers + "}";
        }
    }
}