package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simplified implementation of a Kafka-like broker
 */
public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final Map<String, List<Partition>> topics;
    private final ExecutorService executor;
    private final ServerSocketChannel serverChannel;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    private final ZookeeperClient zkClient;
    /**
     * One ordered replication queue per (topic, partition, follower). Replicas must
     * receive offsets in the order the leader assigned them, so each queue runs on a
     * single thread; separate queues keep a slow follower from blocking the others.
     */
    private final Map<String, ExecutorService> replicationExecutors = new ConcurrentHashMap<>();
    /** How long the leader waits for a follower to answer a replication request. */
    private static final long REPLICATION_TIMEOUT_MS = 10_000;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();

        // Initialize data directory
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        // Initialize ZooKeeper client
        this.zkClient = new ZookeeperClient("localhost", zkPort);
    }

    /**
     * Notify a broker about topic creation
     */
    private void notifyBrokerForTopicCreation(int brokerId, String topic) {
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if (broker == null)
            return;

        executor.submit(() -> {
            try (SocketChannel brokerChannel = SocketChannel.open()) {
                brokerChannel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));

                // Prepare notification
                ByteBuffer request = ByteBuffer.allocate(3 + topic.length());
                request.put(Protocol.TOPIC_NOTIFICATION);
                request.putShort((short) topic.length());
                request.put(topic.getBytes());
                request.flip();

                // Send notification
                brokerChannel.write(request);

                // Read acknowledgment
                ByteBuffer response = ByteBuffer.allocate(1);
                brokerChannel.read(response);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to notify broker " + brokerId + " about topic creation", e);
            }
        });
    }

    /**
     * Handle topic notification from controller
     */
    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        LOGGER.info("Received topic notification for: " + topic);

        // Load topic metadata from ZooKeeper
        try {
            loadTopic(topic);

            // Send acknowledgment
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // Acknowledgment
            response.flip();
            clientChannel.write(response);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);

            // Send error response
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 1); // Error
            response.flip();
            clientChannel.write(response);
        }
    }

    /**
     * Load topic metadata from ZooKeeper
     */
    private void loadTopic(String topic) throws Exception {
        if (topics.containsKey(topic)) {
            LOGGER.info("Topic already loaded: " + topic);
            return;
        }

        String topicPath = "/topics/" + topic;
        if (!zkClient.exists(topicPath)) {
            throw new Exception("Topic does not exist in ZooKeeper: " + topic);
        }

        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;

        List<String> partitionIds = zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = new ArrayList<>();

        for (String partitionId : partitionIds) {
            int id = Integer.parseInt(partitionId);
            String partitionPath = topicPath + "/partitions/" + partitionId;
            String partitionData = zkClient.getData(partitionPath);

            String[] parts = partitionData.split(";");
            int leader = Integer.parseInt(parts[0]);

            List<Integer> followers = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty()) {
                String[] followerIds = parts[1].split(",");
                for (String followerId : followerIds) {
                    if (!followerId.isEmpty()) {
                        followers.add(Integer.parseInt(followerId));
                    }
                }
            }

            // Only a broker that actually replicates the partition gets a log
            // directory. Everyone else keeps the assignment (leader + followers)
            // so it can answer metadata requests and forward writes, but writing a
            // log file here would create an empty log starting at offset 0.
            String partitionDir = topicDir + File.separator + id;
            Partition partition =
                    new Partition(id, leader, followers, partitionDir, storesPartition(leader, followers));
            partitions.add(partition);

            LOGGER.info("Loaded partition " + id + " for topic " + topic +
                    ", leader: " + leader + ", followers: " + followers);
        }

        topics.put(topic, partitions);
        LOGGER.info("Successfully loaded topic: " + topic + " with " + partitions.size() + " partitions");
    }

    /**
     * Whether this broker stores the log of a partition, i.e. whether it is the
     * leader or one of its followers.
     */
    private boolean storesPartition(int leader, List<Integer> followers) {
        if (leader == brokerId) {
            return true;
        }
        for (int follower : followers) {
            if (follower == brokerId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Load all topics from ZooKeeper
     */
    public void loadTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");

            for (String topic : topicNames) {
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);
                }
            }

            LOGGER.info("Loaded " + topics.size() + " topics");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topics", e);
        }
    }

    /**
     * Main entry point for running a broker
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaBroker <brokerId> <host> <port> [zkPort]");
            System.exit(1);
        }

        try {
            int brokerId = Integer.parseInt(args[0]);
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            int zkPort = args.length > 3 ? Integer.parseInt(args[3]) : 2181;

            SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
            broker.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

            System.out.println("SimpleKafka broker started. Press Ctrl+C to stop.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start broker", e);
        }
    }

    /**
     * Start the broker server
     */
    public void start() throws IOException {
        if (isRunning.compareAndSet(false, true)) {
            // Bind to socket
            serverChannel.socket().bind(new InetSocketAddress(brokerHost, brokerPort));
            serverChannel.configureBlocking(false);

            LOGGER.info("SimpleKafka broker started on " + brokerHost + ":" + brokerPort);

            // Register with ZooKeeper
            registerWithZookeeper();

            // Start controller election process
            electController();

            // Load existing topics
            loadTopics();

            // Accept client connections
            executor.submit(this::acceptConnections);
        }
    }

    /**
     * Stop the broker server
     */
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping SimpleKafka broker...");

                // Close server socket
                serverChannel.close();

                // Close all topic partitions
                for (List<Partition> partitions : topics.values()) {
                    for (Partition partition : partitions) {
                        partition.close();
                    }
                }

                // Shut down executor
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);

                // Shut down the replication queues
                for (ExecutorService replication : replicationExecutors.values()) {
                    replication.shutdown();
                }
                replicationExecutors.clear();

                // Close ZooKeeper connection
                zkClient.close();

                LOGGER.info("SimpleKafka broker stopped");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error stopping broker", e);
            }
        }
    }

    /**
     * Register this broker with ZooKeeper
     */
    private void registerWithZookeeper() {
        try {
            zkClient.connect();
            String brokerPath = "/brokers/" + brokerId;
            String brokerData = brokerHost + ":" + brokerPort;
            zkClient.createEphemeralNode(brokerPath, brokerData);

            // Add broker info to local metadata
            BrokerInfo selfInfo = new BrokerInfo(brokerId, brokerHost, brokerPort);
            clusterMetadata.put(brokerId, selfInfo);

            // Watch for other brokers
            zkClient.watchChildren("/brokers", this::onBrokersChanged);

            LOGGER.info("Registered with ZooKeeper at " + zkClient.getConnectString());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }

    /**
     * Handle changes in the broker list from ZooKeeper
     */
    private void onBrokersChanged(List<String> brokerIds) {
        LOGGER.info("Broker change detected. Current brokers: " + brokerIds);

        // Update cluster metadata
        for (String id : brokerIds) {
            try {
                int brokerId = Integer.parseInt(id);
                if (!clusterMetadata.containsKey(brokerId)) {
                    String brokerData = zkClient.getData("/brokers/" + id);
                    String[] hostPort = brokerData.split(":");
                    BrokerInfo info = new BrokerInfo(
                            brokerId,
                            hostPort[0],
                            Integer.parseInt(hostPort[1]));
                    clusterMetadata.put(brokerId, info);
                    LOGGER.info("Added broker: " + info);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to process broker info", e);
            }
        }

        // Remove brokers that have disappeared
        List<Integer> toRemove = new ArrayList<>();
        for (Integer brokerId : clusterMetadata.keySet()) {
            if (!brokerIds.contains(String.valueOf(brokerId))) {
                toRemove.add(brokerId);
            }
        }

        for (Integer brokerId : toRemove) {
            clusterMetadata.remove(brokerId);
            LOGGER.info("Removed broker: " + brokerId);
        }

        // Re-elect controller if needed
        if (!brokerIds.contains(String.valueOf(brokerId)) && isController.get()) {
            isController.set(false);
            LOGGER.info("This broker is no longer in the cluster, giving up controller status");
        } else if (isController.get()) {
            // As controller, rebalance partitions due to cluster changes
            rebalancePartitions();
        } else {
            // Re-attempt controller election
            electController();
        }
    }

    /**
     * Participate in controller election.
     *
     * <p>The election is a single atomic ZooKeeper create: every broker but the
     * winner is told the node already exists, which {@link
     * ZookeeperClient#createEphemeralNode} reports as {@code false}. That is the
     * normal outcome for most brokers and must never be logged as an election
     * failure - only a real ZooKeeper error is.
     */
    private void electController() {
        try {
            String controllerPath = "/controller";

            // Clear a leftover placeholder from an older version before trying; without
            // one this is a cheap no-op read.
            zkClient.deleteEmptyNode(controllerPath);

            // The winner's node can be removed again by a broker that read the
            // placeholder a moment earlier, so a couple of turns are allowed before
            // falling back to a delayed retry.
            for (int attempt = 0; attempt < 3; attempt++) {
                if (claimControllership(controllerPath)) {
                    isController.set(true);
                    LOGGER.info("This broker is now the active controller");

                    // As controller, ensure all topics are properly replicated
                    rebalancePartitions();
                    return;
                }

                // For every broker but one, losing the race is the expected result.
                String controllerId = zkClient.readDataIfPresent(controllerPath);
                if (controllerId == null || controllerId.trim().isEmpty()) {
                    // The node disappeared (or a placeholder was in the way) between our
                    // attempt and our read: go around again rather than watching a node
                    // that is already gone.
                    zkClient.deleteEmptyNode(controllerPath);
                    continue;
                }

                LOGGER.info("Current controller is broker " + controllerId);

                // Watch the controller node for changes
                zkClient.watchNode(controllerPath, this::onControllerChange);
                return;
            }

            scheduleControllerElection(1000);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);

            scheduleControllerElection(2000);
        }
    }

    /**
     * Try to take controllership and confirm afterwards that the node still holds
     * this broker's id: a concurrent stale-placeholder cleanup could have removed it
     * again, and running as a controller nobody can see is worse than retrying.
     */
    private boolean claimControllership(String controllerPath) throws Exception {
        if (!zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId))) {
            return false;
        }
        return String.valueOf(brokerId).equals(zkClient.readDataIfPresent(controllerPath));
    }

    /**
     * Retry the election later without blocking the caller
     */
    private void scheduleControllerElection(long delayMillis) {
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(delayMillis);
                electController();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "controller-election-" + brokerId);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Handle controller change notification
     */
    private void onControllerChange() {
        LOGGER.info("Controller changed, initiating new election");
        electController();
    }

    /**
     * Rebalance partitions across available brokers
     */
    private void rebalancePartitions() {
        if (!isController.get()) {
            return;
        }

        LOGGER.info("Rebalancing partitions across cluster");

        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            for (Partition partition : partitions) {
                // Ensure each partition has a leader
                if (partition.getLeader() == -1 || !clusterMetadata.containsKey(partition.getLeader())) {
                    // Assign a new leader
                    List<Integer> brokers = new ArrayList<>(clusterMetadata.keySet());
                    if (!brokers.isEmpty()) {
                        int newLeader = brokers.get(0);
                        partition.setLeader(newLeader);

                        // Set other brokers as followers
                        List<Integer> followers = new ArrayList<>();
                        for (int i = 1; i < Math.min(brokers.size(), 3); i++) {
                            followers.add(brokers.get(i));
                        }
                        partition.setFollowers(followers);

                        if (newLeader == brokerId && !partition.isLocalReplica()) {
                            LOGGER.warning("Partition " + partition.getId() + " of topic " + topic +
                                    " was reassigned to this broker, but this broker holds no log" +
                                    " for it; writes to it will be rejected");
                        }

                        // Update partition metadata in ZooKeeper
                        updatePartitionMetadata(topic, partition);

                        LOGGER.info("Reassigned partition " + partition.getId() +
                                " of topic " + topic +
                                " to leader " + newLeader +
                                " with followers " + followers);
                    }
                }
            }
        }
    }

    /**
     * Update partition metadata in ZooKeeper
     */
    private void updatePartitionMetadata(String topic, Partition partition) {
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            String data = partition.getLeader() + ";";
            for (int follower : partition.getFollowers()) {
                data += follower + ",";
            }

            if (zkClient.exists(path)) {
                zkClient.setData(path, data);
            } else {
                zkClient.createPersistentNode(path, data);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update partition metadata", e);
        }
    }

    /**
     * Accept client connections
     */
    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);
                    LOGGER.info("Accepted connection from " + clientChannel.getRemoteAddress());

                    // Handle client connection in a separate thread
                    executor.submit(() -> handleClient(clientChannel));
                }

                Thread.sleep(100); // Small pause to prevent CPU spin
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    /**
     * Handle client connection
     */
    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);

                if (bytesRead > 0) {
                    buffer.flip();
                    // Process the message based on protocol
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    // Connection closed by client
                    clientChannel.close();
                    break;
                }

                Thread.sleep(50); // Small pause to prevent CPU spin
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                LOGGER.log(Level.SEVERE, "Error handling client", e);
            }
        } finally {
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }

    /**
     * Process client message based on SimpleKafka wire protocol
     */
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();

        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    /**
     * Handle produce request from client
     */
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Produce request for topic: " + topic + ", partition: " + partition);

        // Check if topic exists
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        // Find the partition
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        // Check if this broker is the leader for the partition
        if (targetPartition.getLeader() != brokerId) {
            // Forward to leader
            forwardProduceToLeader(clientChannel, topic, partition, message, targetPartition.getLeader());
            return;
        }

        // Append the message and hand it to the replication queue as one step, so the
        // followers see offsets in exactly the order the leader assigned them.
        long offset;
        List<CompletableFuture<Void>> acks;
        synchronized (targetPartition) {
            offset = targetPartition.append(message);
            if (offset < 0) {
                Protocol.sendErrorResponse(clientChannel, "Failed to append message");
                return;
            }

            acks = replicateToFollowers(topic, targetPartition, message, offset);
        }

        // acks=all: the client is only told the write succeeded once every follower
        // this broker knows about has durably stored it. The wait happens outside the
        // monitor so other producers for this partition keep moving, and the
        // submission order that the wait depends on was already fixed above.
        //
        // A refused acknowledgement does not roll the leader's copy back - Kafka
        // doesn't either - so the client may retry and end up with the message at a
        // later offset. What must not happen is reporting success for a write that
        // only exists on one broker.
        String replicationFailure = awaitReplication(acks);
        if (replicationFailure != null) {
            LOGGER.warning("Rejecting produce to " + topic + "/" + partition + "@" + offset +
                    ": " + replicationFailure);
            Protocol.sendErrorResponse(clientChannel, replicationFailure);
            return;
        }

        // Send acknowledgment to client
        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) 0); // 0 = success, 1 = error
        response.flip();
        Protocol.writeFully(clientChannel, response);
    }

    /**
     * Block until every follower has stored the message.
     *
     * @return {@code null} when the write is replicated, otherwise the reason the
     *         produce has to be reported as failed
     */
    private String awaitReplication(List<CompletableFuture<Void>> acks) {
        if (acks.isEmpty()) {
            return null; // no follower this broker knows about, nothing to wait for
        }

        try {
            CompletableFuture.allOf(acks.toArray(new CompletableFuture<?>[0]))
                    .get(REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return null;
        } catch (TimeoutException e) {
            return "Timed out waiting for the followers to replicate the message";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "Interrupted while waiting for the followers to replicate the message";
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            String detail = cause.getMessage();
            return "A follower did not replicate the message: " +
                    (detail == null ? cause.getClass().getSimpleName() : detail);
        }
    }

    /**
     * Forward produce request to leader broker
     */
    private void forwardProduceToLeader(SocketChannel clientChannel, String topic, int partition,
            byte[] message, int leaderId) throws IOException {
        BrokerInfo leader = clusterMetadata.get(leaderId);
        if (leader == null) {
            Protocol.sendErrorResponse(clientChannel, "Leader broker not available");
            return;
        }

        try (SocketChannel leaderChannel = SocketChannel.open()) {
            connect(leaderChannel, leader, REPLICATION_TIMEOUT_MS);

            // Prepare forwarded produce request
            // Header: 1 (type) + 2 (topic length) + 4 (partition) + 4 (message length) = 11
            ByteBuffer request = ByteBuffer.allocate(11 + topic.length() + message.length);
            request.put(Protocol.PRODUCE);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(partition);
            request.putInt(message.length);
            request.put(message);
            request.flip();

            // Send request to leader
            Protocol.writeFully(leaderChannel, request);

            // Read the leader's response. The leader now waits for its followers before
            // answering, so this takes longer than it used to - and a single read()
            // would hand back whatever fragment of the frame arrived first.
            ByteBuffer response =
                    Protocol.readProduceResponse(leaderChannel, Protocol.PRODUCE_RESPONSE_TIMEOUT_MS);

            // Forward leader's response back to client
            Protocol.writeFully(clientChannel, response);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to forward produce request to leader", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
        }
    }

    /**
     * Queue the message for every follower of this partition and return one future
     * per follower that has to acknowledge it.
     *
     * <p>Replication runs asynchronously on a per (topic, partition, follower) thread
     * so offsets reach each replica in the order the leader assigned them; the
     * futures let the caller wait for the outcome without giving up that ordering.
     * Followers this broker has never heard of are skipped rather than failed: it
     * cannot replicate to a broker it does not know about, and pretending it could
     * would make every produce to a partition fail during startup.
     */
    private List<CompletableFuture<Void>> replicateToFollowers(String topic, Partition partition,
            byte[] message, long offset) {
        int partitionId = partition.getId();
        List<CompletableFuture<Void>> acks = new ArrayList<>();

        for (int followerId : partition.getFollowers()) {
            if (followerId == brokerId)
                continue; // Skip self

            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null) {
                LOGGER.warning("Cannot replicate partition " + partitionId + " of topic " +
                        topic + ": follower broker " + followerId + " is not known to this broker");
                continue;
            }

            CompletableFuture<Void> acked = new CompletableFuture<>();
            acks.add(acked);
            replicationExecutor(topic, partitionId, followerId).submit(
                    () -> replicateMessage(topic, partitionId, follower, offset, message, acked));
        }

        return acks;
    }

    /**
     * Ordered replication queue for one (topic, partition, follower) triple
     */
    private ExecutorService replicationExecutor(String topic, int partitionId, int followerId) {
        String key = topic + "/" + partitionId + "->" + followerId;
        return replicationExecutors.computeIfAbsent(key, k -> {
            ThreadFactory factory = r -> {
                Thread thread = new Thread(r, "replication-" + k);
                thread.setDaemon(true);
                return thread;
            };
            return Executors.newSingleThreadExecutor(factory);
        });
    }

    /**
     * Send one message to a follower and, if the follower reports that it is behind,
     * re-send everything it is missing starting from its own log end offset.
     *
     * <p>The outcome is reported through {@code acked}, which the leader waits on
     * before acknowledging the produce to its client.
     */
    private void replicateMessage(String topic, int partitionId, BrokerInfo follower,
            long offset, byte[] message, CompletableFuture<Void> acked) {
        try {
            ReplicationAck ack = sendReplicateRequest(topic, partitionId, follower, offset, message);

            if (ack.status == Protocol.REPLICATE_ACK) {
                LOGGER.info("Replication of " + topic + "/" + partitionId + "@" + offset +
                        " to broker " + follower.getId() + " succeeded");
                acked.complete(null);
                return;
            }

            if (ack.status == Protocol.REPLICATE_NACK && ack.logEndOffset >= 0
                    && ack.logEndOffset <= offset) {
                LOGGER.warning("Broker " + follower.getId() + " is behind for " + topic + "/" +
                        partitionId + ": its log ends at " + ack.logEndOffset + " but the leader wrote " +
                        offset + ". Catching it up.");
                if (catchUpFollower(topic, partitionId, follower, ack.logEndOffset, offset)) {
                    acked.complete(null);
                } else {
                    acked.completeExceptionally(new IOException("broker " + follower.getId() +
                            " could not be caught up through offset " + offset));
                }
                return;
            }

            LOGGER.severe("Replication of " + topic + "/" + partitionId + "@" + offset +
                    " to broker " + follower.getId() + " failed with status " + ack.status);
            acked.completeExceptionally(new IOException("broker " + follower.getId() +
                    " answered the replication request with status " + ack.status));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Replication to broker " + follower.getId() + " failed", e);
            acked.completeExceptionally(e);
        }
    }

    /**
     * Re-send everything the follower is missing, from its log end offset up to and
     * including {@code toOffset}.
     *
     * @return true when the follower has confirmed every message through
     *         {@code toOffset}
     */
    private boolean catchUpFollower(String topic, int partitionId, BrokerInfo follower,
            long fromOffset, long toOffset) throws IOException {
        Partition partition = findPartition(topic, partitionId);
        if (partition == null) {
            LOGGER.severe("Cannot catch up broker " + follower.getId() + ": partition " +
                    partitionId + " of topic " + topic + " is no longer loaded");
            return false;
        }

        long cursor = fromOffset;
        while (cursor <= toOffset) {
            List<byte[]> batch = partition.readMessages(cursor, 64 * 1024);
            if (batch.isEmpty()) {
                LOGGER.severe("Cannot catch up broker " + follower.getId() + ": leader has no data at offset " + cursor);
                return false;
            }

            for (byte[] pending : batch) {
                if (cursor > toOffset) {
                    break;
                }

                ReplicationAck ack = sendReplicateRequest(topic, partitionId, follower, cursor, pending);
                if (ack.status != Protocol.REPLICATE_ACK) {
                    LOGGER.severe("Catch-up of broker " + follower.getId() + " for " + topic + "/" +
                            partitionId + " stopped at offset " + cursor + " with status " + ack.status);
                    return false;
                }
                cursor++;
            }
        }

        LOGGER.info("Broker " + follower.getId() + " caught up for " + topic + "/" + partitionId +
                " through offset " + toOffset);
        return true;
    }

    /**
     * Send a single replication request and read the follower's reply
     */
    private ReplicationAck sendReplicateRequest(String topic, int partitionId, BrokerInfo follower,
            long offset, byte[] message) throws IOException {
        try (SocketChannel followerChannel = SocketChannel.open()) {
            connect(followerChannel, follower, REPLICATION_TIMEOUT_MS);

            // Header: 1 (type) + 2 (topic length) + 4 (partition) + 8 (offset) + 4 (message length) = 19
            ByteBuffer request = ByteBuffer.allocate(19 + topic.length() + message.length);
            request.put(Protocol.REPLICATE);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(partitionId);
            request.putLong(offset);
            request.putInt(message.length);
            request.put(message);
            request.flip();

            Protocol.writeFully(followerChannel, request);

            ByteBuffer response = ByteBuffer.allocate(Protocol.REPLICATION_RESPONSE_SIZE);
            Protocol.readFully(followerChannel, response, REPLICATION_TIMEOUT_MS);
            response.flip();

            return new ReplicationAck(response.get(), response.getLong());
        }
    }

    /**
     * Connect with a deadline. A plain blocking {@code connect()} can hang for
     * minutes against an unreachable host, which would strand this replication queue
     * and turn every later produce for the partition into a timeout.
     */
    private static void connect(SocketChannel channel, BrokerInfo broker, long timeoutMillis)
            throws IOException {
        channel.configureBlocking(false);
        try {
            if (!channel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()))) {
                long deadline = System.currentTimeMillis() + timeoutMillis;
                while (!channel.finishConnect()) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new IOException("Timed out connecting to broker " + broker.getId() +
                                " at " + broker.getHost() + ":" + broker.getPort());
                    }
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while connecting to broker " + broker.getId(), e);
                    }
                }
            }
        } catch (IOException e) {
            throw new IOException("Cannot reach broker " + broker.getId() + " at " +
                    broker.getHost() + ":" + broker.getPort() + ": " + e.getMessage(), e);
        }
        channel.configureBlocking(true);
    }

    /**
     * Reply to a replication request
     */
    private void replyReplication(SocketChannel clientChannel, byte status, long logEndOffset) throws IOException {
        Protocol.writeFully(clientChannel, Protocol.encodeReplicationResponse(status, logEndOffset));
    }

    /**
     * Handle replication request from leader
     */
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Replication request for topic: " + topic + ", partition: " + partitionId + ", offset: " + offset);

        Partition targetPartition = findPartition(topic, partitionId);
        if (targetPartition == null) {
            replyReplication(clientChannel, Protocol.REPLICATE_FAILED, 0);
            return;
        }

        // The leader owns the offset: write at exactly the position it picked, or say
        // where this replica's log ends so the leader can re-send what is missing.
        long result = targetPartition.appendAt(offset, message);

        byte status;
        if (result == offset) {
            status = Protocol.REPLICATE_ACK;
        } else if (result == -1) {
            status = Protocol.REPLICATE_NACK;
        } else {
            status = Protocol.REPLICATE_FAILED;
        }

        replyReplication(clientChannel, status, targetPartition.getLogEndOffset());
    }

    /**
     * Find a partition by topic name and id, or null when this broker does not have it
     */
    private Partition findPartition(String topic, int partitionId) {
        List<Partition> partitions = topics.get(topic);
        if (partitions == null) {
            return null;
        }

        for (Partition partition : partitions) {
            if (partition.getId() == partitionId) {
                return partition;
            }
        }

        return null;
    }

    /**
     * A follower's reply to a replication request
     */
    private static class ReplicationAck {
        private final byte status;
        private final long logEndOffset;

        ReplicationAck(byte status, long logEndOffset) {
            this.status = status;
            this.logEndOffset = logEndOffset;
        }
    }

    /**
     * Handle fetch request from client
     */
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        LOGGER.info("Fetch request for topic: " + topic + ", partition: " + partition +
                ", offset: " + offset + ", maxBytes: " + maxBytes);

        // Check if topic exists
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        // Find the partition
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        // Check if the offset is valid
        if (offset >= targetPartition.getLogEndOffset()) {
            // No messages available at this offset
            ByteBuffer response = ByteBuffer.allocate(5);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0); // 0 messages
            response.flip();
            clientChannel.write(response);
            return;
        }

        // Read messages from log
        List<byte[]> messages = targetPartition.readMessages(offset, maxBytes);

        // Send response
        int totalSize = 5; // 1 byte for response type, 4 bytes for message count
        for (byte[] msg : messages) {
            totalSize += 12 + msg.length; // 8 bytes for offset, 4 bytes for length, plus message bytes
        }

        ByteBuffer response = ByteBuffer.allocate(totalSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset);
            response.putInt(msg.length);
            response.put(msg);
            currentOffset++;
        }

        response.flip();
        clientChannel.write(response);
    }

    /**
     * Handle metadata request from client
     */
    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // Prepare response with metadata
        int size = 5; // 1 byte for response type, 4 bytes for topic count

        // Calculate size for topics metadata
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            size += 6 + entry.getKey().length(); // 2 bytes for length, string, 4 bytes for partition count

            // Add size for each partition
            size += entry.getValue().size() * 12; // 4 bytes for id, 4 bytes for leader, 4 bytes for follower count

            // Add size for followers
            for (Partition partition : entry.getValue()) {
                size += partition.getFollowers().size() * 4; // 4 bytes per follower ID
            }
        }

        // Add size for brokers metadata
        size += 4; // 4 bytes for broker count
        size += clusterMetadata.size() * 10; // 4 bytes for id, 2 bytes for host length, 4 bytes for port

        // Add estimated size for broker hostnames
        for (BrokerInfo broker : clusterMetadata.values()) {
            size += broker.getHost().length();
        }

        ByteBuffer response = ByteBuffer.allocate(size);
        response.put(Protocol.METADATA_RESPONSE);

        // Add broker metadata
        response.putInt(clusterMetadata.size());
        for (BrokerInfo broker : clusterMetadata.values()) {
            response.putInt(broker.getId());
            response.putShort((short) broker.getHost().length());
            response.put(broker.getHost().getBytes());
            response.putInt(broker.getPort());
        }

        // Add topic metadata
        response.putInt(topics.size());
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            response.putShort((short) topic.length());
            response.put(topic.getBytes());
            response.putInt(partitions.size());

            for (Partition partition : partitions) {
                response.putInt(partition.getId());
                response.putInt(partition.getLeader());

                List<Integer> followers = partition.getFollowers();
                response.putInt(followers.size());
                for (Integer follower : followers) {
                    response.putInt(follower);
                }
            }
        }

        response.flip();
        clientChannel.write(response);
    }

    /**
     * Handle create topic request from client
     */
    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        LOGGER.info("Create topic request: " + topic +
                ", partitions: " + numPartitions +
                ", replication: " + replicationFactor);

        // Check if topic already exists
        if (topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic already exists");
            return;
        }

        // Validate parameters
        if (numPartitions <= 0 || replicationFactor <= 0 ||
                replicationFactor > clusterMetadata.size()) {
            Protocol.sendErrorResponse(clientChannel, "Invalid topic configuration");
            return;
        }

        // As controller, create the topic
        if (isController.get()) {
            createTopic(topic, numPartitions, replicationFactor);

            // Send success response
            ByteBuffer response = ByteBuffer.allocate(2);
            response.put(Protocol.CREATE_TOPIC_RESPONSE);
            response.put((byte) 0); // 0 = success
            response.flip();
            clientChannel.write(response);
        } else {
            // Forward to controller
            forwardCreateTopicToController(clientChannel, topic, numPartitions, replicationFactor);
        }
    }

    /**
     * Forward create topic request to controller
     */
    private void forwardCreateTopicToController(SocketChannel clientChannel, String topic,
            int numPartitions, short replicationFactor) throws IOException {
        // Find controller
        int controllerId = -1;
        try {
            String controllerData = zkClient.readDataIfPresent("/controller");
            if (controllerData == null || controllerData.trim().isEmpty()) {
                // Not an error: nobody has won the election yet.
                LOGGER.warning("No active controller yet, cannot create topic " + topic);
                Protocol.sendErrorResponse(clientChannel, "Controller not available");
                return;
            }
            controllerId = Integer.parseInt(controllerData);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get controller info", e);
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        BrokerInfo controller = clusterMetadata.get(controllerId);
        if (controller == null) {
            Protocol.sendErrorResponse(clientChannel, "Controller broker not available");
            return;
        }

        try (SocketChannel controllerChannel = SocketChannel.open()) {
            controllerChannel.connect(new InetSocketAddress(controller.getHost(), controller.getPort()));

            // Prepare forwarded create topic request
            ByteBuffer request = ByteBuffer.allocate(9 + topic.length());
            request.put(Protocol.CREATE_TOPIC);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(numPartitions);
            request.putShort(replicationFactor);
            request.flip();

            // Send request to controller
            controllerChannel.write(request);

            // Read response from controller
            ByteBuffer response = ByteBuffer.allocate(2);
            controllerChannel.read(response);
            response.flip();

            // Forward controller's response back to client
            clientChannel.write(response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller");
        }
    }

    /**
     * Create a new topic with the specified configuration
     */
    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            LOGGER.warning("Only the controller can create topics");
            return;
        }

        try {
            // Topic/partition directories are created lazily by Partition itself,
            // and only for the partitions this broker actually replicates.
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;

            // Create topic in ZooKeeper
            String topicPath = "/topics/" + topic;
            if (!zkClient.exists(topicPath)) {
                zkClient.createPersistentNode(topicPath, "");
                zkClient.createPersistentNode(topicPath + "/partitions", "");
            }

            // Create partitions
            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());

            for (int i = 0; i < numPartitions; i++) {
                int partitionId = i;

                // Select leader and followers
                int leaderIndex = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex);

                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor; j++) {
                    int followerIndex = (leaderIndex + j) % brokerIds.size();
                    followers.add(brokerIds.get(followerIndex));
                }

                // Create partition
                String partitionDir = topicDir + File.separator + partitionId;
                Partition partition = new Partition(
                        partitionId, leaderId, followers, partitionDir,
                        storesPartition(leaderId, followers));
                partitions.add(partition);

                // Store partition metadata in ZooKeeper
                String partitionPath = topicPath + "/partitions/" + partitionId;
                String partitionData = leaderId + ";";
                for (int follower : followers) {
                    partitionData += follower + ",";
                }

                zkClient.createPersistentNode(partitionPath, partitionData);

                LOGGER.info("Created partition " + partitionId +
                        " for topic " + topic +
                        " with leader " + leaderId +
                        " and followers " + followers);
            }

            // Add topic to broker's metadata
            topics.put(topic, partitions);

            // Notify all brokers to load the topic
            for (int brokerId : brokerIds) {
                if (brokerId != this.brokerId) {
                    notifyBrokerForTopicCreation(brokerId, topic);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
        }
    }
}