package com.simplekafka.broker;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
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
    /**
     * The largest request the broker will reassemble. The frame is measured from the
     * request's own header rather than assumed, so this is a ceiling rather than a
     * buffer size - the analogue of Kafka's {@code message.max.bytes}.
     */
    private static final int MAX_REQUEST_BYTES = 1024 * 1024;
    /**
     * Where partition assignments read back from ZooKeeper get applied. Watches fire
     * on ZooKeeper's event thread, and the blocking reads needed here would stall
     * every other watch this broker has open.
     */
    private final ExecutorService metadataExecutor;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this(brokerId, host, port, new ZookeeperClient("localhost", zkPort));
    }

    /**
     * Build a broker around a supplied ZooKeeper client.
     *
     * <p>Exists so a test can hand it a client that records what the broker asks for and
     * in what order - the ordering of a watch against a read is invisible from outside,
     * and it is the whole guarantee.
     */
    SimpleKafkaBroker(int brokerId, String host, int port, ZookeeperClient zkClient) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
        this.metadataExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "partition-metadata-" + brokerId);
            thread.setDaemon(true);
            return thread;
        });
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();

        // Initialize data directory
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        // Initialize ZooKeeper client. A session that ends takes this broker's ephemeral
        // nodes and every watch it registered with it, so it has to be told and put them
        // back.
        this.zkClient = zkClient;
        this.zkClient.setSessionListener(this::onSessionExpired);
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
                ByteBuffer request = Protocol.encodeTopicNotification(topic);

                // Send notification
                Protocol.writeFully(brokerChannel, request);

                // Read acknowledgment
                ByteBuffer response = ByteBuffer.allocate(1);
                Protocol.readFully(brokerChannel, response, Protocol.DEFAULT_IO_TIMEOUT_MS);
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
     * Load topic metadata from ZooKeeper.
     *
     * <p>Synchronized because the check and the put have to be one step. Two
     * notifications for the same topic - a controller that sent one twice, or a second
     * controller - are each handled on their own client thread, and left unsynchronized
     * both would find the topic absent, both would build a Partition per partition, and
     * both would put them: the loser's objects are dropped from the map but keep an open
     * file handle on the same log and their own armed watch, so two writers exist for one
     * file and only one of them is reachable.
     */
    synchronized void loadTopic(String topic) throws Exception {
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
            Assignment assignment = parseAssignment(zkClient.getData(partitionPath));

            // Only a broker that actually replicates the partition gets a log
            // directory. Everyone else keeps the assignment (leader + followers)
            // so it can answer metadata requests and forward writes, but writing a
            // log file here would create an empty log starting at offset 0.
            String partitionDir = topicDir + File.separator + id;
            Partition partition = new Partition(id, assignment.leader, assignment.followers, partitionDir,
                    storesPartition(assignment.leader, assignment.followers));
            partitions.add(partition);

            LOGGER.info("Loaded partition " + id + " for topic " + topic +
                    ", leader: " + assignment.leader + ", followers: " + assignment.followers);

            watchPartitionAssignment(topic, partition);
        }

        topics.put(topic, partitions);
        LOGGER.info("Successfully loaded topic: " + topic + " with " + partitions.size() + " partitions");
    }

    /**
     * The most partitions one create-topic request may ask for. The controller creates a
     * ZooKeeper node and a Partition object per partition, so an unbounded count is a way
     * to exhaust a broker from one small request.
     */
    static final int MAX_PARTITIONS_PER_TOPIC = 1000;

    /** The most a client may ask for in one fetch, matching the reply's own caps. */
    static final int MAX_FETCH_BYTES = 16 * 1024 * 1024;

    /** Ceiling on one fetch reply, per-record framing included. */
    static final int MAX_FETCH_REPLY_BYTES = 32 * 1024 * 1024;

    /**
     * @return why the configuration is unacceptable, or null when it is fine
     */
    static String validateTopicConfig(int numPartitions, short replicationFactor, int knownBrokers) {
        if (numPartitions <= 0) {
            return "A topic needs at least one partition";
        }
        if (numPartitions > MAX_PARTITIONS_PER_TOPIC) {
            return "Too many partitions: " + numPartitions +
                    " (the limit is " + MAX_PARTITIONS_PER_TOPIC + ")";
        }
        if (replicationFactor <= 0) {
            return "Replication factor must be at least 1";
        }
        if (replicationFactor > knownBrokers) {
            return "Replication factor " + replicationFactor +
                    " exceeds the " + knownBrokers + " brokers in the cluster";
        }
        return null;
    }

    /**
     * Clamp a client's requested fetch size. The value arrives in a four byte field of an
     * otherwise bounded request and is handed straight to the reader as a budget, so an
     * enormous one would pull a whole partition into memory.
     */
    static int sanitizeFetchMaxBytes(int maxBytes) {
        return Math.max(0, Math.min(maxBytes, MAX_FETCH_BYTES));
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

                // Shut down the assignment watcher before closing ZooKeeper, so no
                // watch callback can try to re-arm itself against a dead handle
                metadataExecutor.shutdown();
                metadataExecutor.awaitTermination(5, TimeUnit.SECONDS);

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
            joinCluster();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }

    /**
     * Announce this broker and start watching the rest of the cluster.
     *
     * <p>Separate from connecting because a session that expired has to do all of this
     * again - the ephemeral node and the watch went with the session - and must not open
     * a second connection to do it.
     */
    private void joinCluster() throws Exception {
        String brokerPath = "/brokers/" + brokerId;
        if (!zkClient.createEphemeralNode(brokerPath, brokerHost + ":" + brokerPort)) {
            // Either a previous session of ours has not been reaped yet, or two brokers
            // share an id. Either way the node in ZooKeeper is not this session's, so the
            // cluster will stop seeing this broker as soon as that session ends.
            LOGGER.warning("Broker " + brokerId + " already has a /brokers node, which belongs to " +
                    "a session that has not expired yet; this broker is not visible under its own id");
        }

        clusterMetadata.put(brokerId, new BrokerInfo(brokerId, brokerHost, brokerPort));
        zkClient.watchChildren("/brokers", this::onBrokersChanged);

        LOGGER.info("Registered with ZooKeeper at " + zkClient.getConnectString());
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
                    if (isController.compareAndSet(false, true)) {
                        LOGGER.info("This broker is now the active controller");
                    }

                    // Watch the node we just took. Losing it is precisely the event the
                    // flag has to follow, and the winner otherwise only ends up watching
                    // it by accident - when a second election attempt of its own happens
                    // to lose to the node it already holds.
                    zkClient.watchNode(controllerPath, this::onControllerChange);

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
        relinquishControllershipIfLost();
        electController();
    }

    /**
     * Give up controllership if /controller no longer names this broker.
     *
     * <p>The flag was otherwise only cleared when this broker disappeared from /brokers,
     * which does not cover the node going away underneath it: its session ending removes
     * the node, the flag stays set, and the cluster has two brokers acting as controller
     * - both writing assignments and both answering create-topic as the authority.
     *
     * <p>A read that fails is deliberately not treated as losing the node: a transient
     * ZooKeeper error is not evidence about who holds it.
     */
    private void relinquishControllershipIfLost() {
        if (!isController.get()) {
            return;
        }

        try {
            String holder = zkClient.readDataIfPresent("/controller");
            if (!String.valueOf(brokerId).equals(holder)) {
                isController.set(false);
                LOGGER.warning("This broker is no longer the controller" +
                        (holder == null ? "; the /controller node is gone" : "; it is now broker " + holder));
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not check whether this broker is still the controller", e);
        }
    }

    /**
     * Put back everything a ZooKeeper session ending takes with it.
     *
     * <p>An expired session removes this broker's ephemeral nodes - its /brokers entry,
     * and its claim on /controller - and every watch registered on it. ZookeeperClient
     * rebuilds the connection and nothing else, because it has no idea what its owner
     * registered, so without this the broker stays invisible to the cluster and deaf to
     * every notification until it is restarted.
     */
    private void onSessionExpired() {
        if (!isRunning.get()) {
            return;
        }

        LOGGER.warning("ZooKeeper session expired; re-registering this broker");
        isController.set(false); // whatever the old session said about that died with it

        try {
            joinCluster();
            electController();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to re-register after the ZooKeeper session expired", e);
        }
    }

    /** Whether this broker currently believes it is the controller. */
    boolean isController() {
        return isController.get();
    }

    /**
     * Elect a leader for every partition whose leader has dropped out of the cluster.
     *
     * <p>The replica set - whoever was assigned to the partition - is what says who
     * actually holds the log, so a failover moves the leader <em>within</em> that set
     * and never invents one outside it. A broker outside it would be elected to lead a
     * partition whose log it does not have, and every message the real replicas stored
     * would become invisible. Kafka makes the same choice and calls the alternative
     * "unclean leader election".
     *
     * <p>What this deliberately does not do is order the surviving replicas by log end
     * offset. That requires each replica to publish how far it has got - an in-sync
     * replica set - and without one every survivor has to be treated as equally caught
     * up, which here they are, because the leader only acknowledges a produce once
     * every replica has stored it.
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
                if (partition.getLeader() != -1 && clusterMetadata.containsKey(partition.getLeader())) {
                    continue; // still led by a broker that is up
                }

                // The replica set: whoever the assignment gave this partition. The old
                // leader is included because it may have come back.
                List<Integer> replicas = new ArrayList<>();
                if (partition.getLeader() != -1) {
                    replicas.add(partition.getLeader());
                }
                replicas.addAll(partition.getFollowers());

                List<Integer> alive = new ArrayList<>();
                for (int replica : replicas) {
                    if (clusterMetadata.containsKey(replica) && !alive.contains(replica)) {
                        alive.add(replica);
                    }
                }

                if (alive.isEmpty()) {
                    // Refuse to guess: an assignment that never existed carries no
                    // replication factor to honour, and one whose every replica is gone
                    // has nothing to elect from. Either way the answer is "still down",
                    // not "whoever happens to be running".
                    LOGGER.warning("Partition " + partition.getId() + " of topic " + topic + " has "
                            + (replicas.isEmpty() ? "no replica assignment" : "no live replica (assigned " + replicas + ")")
                            + "; leaving it unled until one appears");
                    continue;
                }

                // Any survivor will do. Without an ISR there is nothing to rank them by,
                // so the assignment's own order decides - arbitrary, but every candidate
                // at least holds this partition's log, which no outsider can claim.
                int newLeader = alive.get(0);
                partition.setLeader(newLeader);

                // The replica set itself is left exactly as assigned: a follower that is
                // merely down keeps its place so it can catch up when it returns, and a
                // failover must not quietly rewrite the replication factor.
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
                        ", chosen from replica set " + replicas +
                        "; followers stay at " + partition.getFollowers());
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
     * A partition's leader plus the brokers that replicate it.
     */
    private static final class Assignment {
        private final int leader;
        private final List<Integer> followers;

        private Assignment(int leader, List<Integer> followers) {
            this.leader = leader;
            this.followers = followers;
        }
    }

    /**
     * Parse the {@code leader;follower,follower,} blob a partition's ZooKeeper node
     * holds. One shape, parsed in one place, so a leader change read back by a watch
     * and a topic read at startup cannot end up disagreeing.
     */
    private static Assignment parseAssignment(String data) {
        String[] parts = data.split(";");
        int leader = Integer.parseInt(parts[0].trim());

        List<Integer> followers = new ArrayList<>();
        if (parts.length > 1 && !parts[1].isEmpty()) {
            for (String followerId : parts[1].split(",")) {
                if (!followerId.isEmpty()) {
                    followers.add(Integer.parseInt(followerId));
                }
            }
        }

        return new Assignment(leader, followers);
    }

    /**
     * Watch a partition's assignment in ZooKeeper so that a leader change decided by
     * the controller reaches this broker.
     *
     * <p>Without this the controller would be the only broker that knows the new
     * leader: everyone else would keep answering metadata with, and forwarding writes
     * to, a broker that is gone. Watches are one-shot, so every notification re-arms
     * itself, and the reading happens on {@link #metadataExecutor} rather than on
     * ZooKeeper's event thread, which must never be blocked.
     */
    private void watchPartitionAssignment(String topic, Partition partition) {
        if (!isRunning.get()) {
            return;
        }

        String path = "/topics/" + topic + "/partitions/" + partition.getId();
        try {
            zkClient.watchNode(path, () -> {
                if (!isRunning.get()) {
                    return;
                }
                metadataExecutor.submit(() -> {
                    // Arm the next watch before reading, the same way watchChildren does.
                    // A watch is one-shot, so a change that lands between the read and a
                    // later re-arm produces no event here at all - and this is the only
                    // way this broker learns that a partition's leader moved, so it would
                    // go on answering metadata with, and forwarding writes to, a broker
                    // that is gone. Registering first can only cost a redundant
                    // notification, which applyPartitionAssignment already ignores.
                    watchPartitionAssignment(topic, partition);
                    try {
                        applyPartitionAssignment(topic, partition);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Failed to apply the assignment of "
                                + topic + "/" + partition.getId(), e);
                    }
                });
            });
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to watch the assignment of " + topic + "/" + partition.getId()
                    + "; retrying", e);
            scheduleWatchRetry(topic, partition);
        }
    }

    /**
     * Try to arm an assignment watch again shortly.
     *
     * <p>Failing to arm it leaves the partition pointed at whatever broker it last
     * heard about, with nothing to correct it, so a single ZooKeeper hiccup at startup
     * would otherwise be permanent for that partition.
     */
    private void scheduleWatchRetry(String topic, Partition partition) {
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(1000);
                watchPartitionAssignment(topic, partition);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "partition-watch-retry-" + brokerId);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Read a partition's assignment back from ZooKeeper and take it on if it differs
     * from the one this broker is running with.
     */
    private void applyPartitionAssignment(String topic, Partition partition) throws Exception {
        String path = "/topics/" + topic + "/partitions/" + partition.getId();
        String data = zkClient.readDataIfPresent(path);
        if (data == null || data.isEmpty()) {
            return; // gone or never written; keep the assignment we already have
        }

        Assignment assignment = parseAssignment(data);
        if (assignment.leader == partition.getLeader()
                && assignment.followers.equals(partition.getFollowers())) {
            return; // nothing moved
        }

        partition.setLeader(assignment.leader);
        partition.setFollowers(assignment.followers);

        LOGGER.info("Learned the new assignment of " + topic + "/" + partition.getId()
                + " from ZooKeeper: leader " + assignment.leader
                + ", followers " + assignment.followers
                + (assignment.leader == brokerId ? " - this broker now leads it" : ""));
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
     *
     * <p>Requests are reassembled instead of read in one shot. The wire carries no
     * length prefix, one {@code read()} returns whatever fragment happened to be in
     * the socket buffer, and a fixed buffer would put a hard ceiling on the size of a
     * produced message. So the frame is measured from the type byte plus the lengths
     * in the request's own header, only that frame is handed to the parser, and
     * whatever arrived behind it stays buffered for the next request.
     */
    private void handleClient(SocketChannel clientChannel) {
        ByteBuffer pending = ByteBuffer.allocate(1024);
        long silentSince = 0L;

        try {
            while (clientChannel.isOpen() && isRunning.get()) {
                int frameLength = requestLength(pending);

                if (frameLength >= 0 && pending.position() >= frameLength) {
                    silentSince = 0L;
                    ByteBuffer frame = pending.duplicate();
                    frame.clear();
                    frame.limit(frameLength);
                    // The frame must be parsed before the buffer is compacted: duplicate()
                    // shares the backing array, so dropConsumedBytes' arraycopy would
                    // overwrite this frame's first bytes with the request behind it.
                    processClientMessage(clientChannel, frame);
                    dropConsumedBytes(pending, frameLength);
                    continue;
                }

                if (frameLength > pending.capacity() || !pending.hasRemaining()) {
                    pending = grow(pending, frameLength > 0 ? frameLength : pending.capacity() + 1);
                }

                int bytesRead = clientChannel.read(pending);
                if (bytesRead < 0) {
                    // Connection closed by client
                    clientChannel.close();
                    break;
                }
                if (bytesRead > 0) {
                    silentSince = 0L;
                    continue;
                }

                // Nothing arrived. A connection with no bytes at all is simply idle and
                // worth waiting for; one already holding half a request will never
                // finish it on its own, so give up instead of spinning on it forever.
                if (pending.position() > 0) {
                    long now = System.currentTimeMillis();
                    if (silentSince == 0L) {
                        silentSince = now;
                    } else if (now - silentSince > Protocol.DEFAULT_IO_TIMEOUT_MS) {
                        throw new IOException("Timed out halfway through a request");
                    }
                }
                Thread.sleep(5);
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                if (isClientDisconnect(e)) {
                    // A client that vanishes mid-connection is a normal end to a
                    // connection, not a broker fault: a consumer that exits while a
                    // fetch is in flight leaves a reset behind, and reporting that as
                    // SEVERE makes a healthy cluster look broken.
                    LOGGER.info("Client connection ended: " + e);
                } else {
                    LOGGER.log(Level.SEVERE, "Error handling client", e);
                }
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
     * Whether an exception means "the peer went away" rather than "this broker
     * failed". Every one of these is delivered at the socket layer, so it says
     * nothing about whether the request itself was handled correctly.
     */
    private static boolean isClientDisconnect(Throwable e) {
        return e instanceof SocketException
                || e instanceof ClosedChannelException
                || e instanceof AsynchronousCloseException;
    }

    /**
     * How long the request sitting at the front of {@code buffer} is, or -1 when not
     * enough of it has arrived to tell.
     *
     * <p>Every request type carries its own lengths, so a frame can be measured
     * without a length prefix - but only once its header has been read, which is why
     * the answer is "keep reading" until then.
     *
     * <p>Keep this in step with {@link #processClientMessage}: every type it dispatches
     * has to be measured here, or the parser is handed a truncated frame and fails
     * partway through a request it was told was complete.
     *
     * @throws IOException if the header describes a request that cannot be honoured
     */
    private static int requestLength(ByteBuffer buffer) throws IOException {
        int available = buffer.position();
        if (available < 1) {
            return -1;
        }

        byte[] bytes = buffer.array();
        switch (bytes[0]) {
            case Protocol.METADATA:
                return checkRequestLength(1);
            case Protocol.TOPIC_NOTIFICATION: {
                // [type][topic length][topic]
                if (available < 3) {
                    return -1;
                }
                return checkRequestLength(3 + unsignedShort(bytes, 1));
            }
            case Protocol.FETCH: {
                // [type][topic length][topic][partition][offset][max bytes]
                if (available < 3) {
                    return -1;
                }
                return checkRequestLength(19 + unsignedShort(bytes, 1));
            }
            case Protocol.CREATE_TOPIC: {
                // [type][topic length][topic][partition count][replication factor]
                if (available < 3) {
                    return -1;
                }
                return checkRequestLength(9 + unsignedShort(bytes, 1));
            }
            case Protocol.PRODUCE: {
                // [type][topic length][topic][partition][message length][message]
                if (available < 3) {
                    return -1;
                }
                int topicLength = unsignedShort(bytes, 1);
                if (available < 11 + topicLength) {
                    return -1;
                }
                return checkRequestLength(11 + topicLength + readInt(bytes, 7 + topicLength));
            }
            case Protocol.REPLICATE: {
                // [type][topic length][topic][partition][offset][message length][message]
                if (available < 3) {
                    return -1;
                }
                int topicLength = unsignedShort(bytes, 1);
                if (available < 19 + topicLength) {
                    return -1;
                }
                return checkRequestLength(19 + topicLength + readInt(bytes, 15 + topicLength));
            }
            default:
                // An unknown type has no header to measure, so consume just the type byte
                // and let the dispatcher answer with an error, exactly as it did before.
                return 1;
        }
    }

    private static int checkRequestLength(int length) throws IOException {
        if (length <= 0 || length > MAX_REQUEST_BYTES) {
            throw new IOException("Implausible request length: " + length);
        }
        return length;
    }

    private static int unsignedShort(byte[] bytes, int index) {
        return ((bytes[index] & 0xFF) << 8) | (bytes[index + 1] & 0xFF);
    }

    private static int readInt(byte[] bytes, int index) {
        return ((bytes[index] & 0xFF) << 24) | ((bytes[index + 1] & 0xFF) << 16)
                | ((bytes[index + 2] & 0xFF) << 8) | (bytes[index + 3] & 0xFF);
    }

    /**
     * Remove the first {@code length} bytes of an accumulation buffer, keeping any
     * request that arrived behind them.
     */
    private static void dropConsumedBytes(ByteBuffer buffer, int length) {
        int rest = buffer.position() - length;
        if (rest > 0) {
            System.arraycopy(buffer.array(), length, buffer.array(), 0, rest);
        }
        buffer.position(rest);
    }

    /**
     * Copy an accumulation buffer into a larger one so a request bigger than the
     * current capacity can be reassembled.
     */
    private static ByteBuffer grow(ByteBuffer buffer, int required) throws IOException {
        if (required > MAX_REQUEST_BYTES) {
            throw new IOException("Request larger than " + MAX_REQUEST_BYTES + " bytes");
        }
        int size = Math.min(Math.max(buffer.capacity() * 2, required), MAX_REQUEST_BYTES);

        ByteBuffer bigger = ByteBuffer.allocate(size);
        buffer.flip();
        bigger.put(buffer);
        return bigger;
    }

    /**
     * Process client message based on SimpleKafka wire protocol
     *
     * <p>Every case here needs a matching measurement in {@link #requestLength},
     * otherwise the frame arrives truncated and parsing stops halfway through.
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

            // A forwarded produce is byte for byte the request the client would have sent.
            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);

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

            ByteBuffer request = Protocol.encodeReplicateRequest(topic, partitionId, offset, message);

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
        int maxBytes = sanitizeFetchMaxBytes(buffer.getInt());

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

        if (targetPartition.getLeader() != brokerId) {
            // Only the leader answers reads. A follower's log may be behind, or absent
            // altogether on a broker outside the replica set, and an empty answer would
            // be indistinguishable from a genuinely empty partition - so refuse instead
            // of handing back a hole the consumer cannot explain.
            Protocol.sendErrorResponse(clientChannel,
                    "Broker " + brokerId + " is not the leader for " + topic + "/" + partition
                            + "; leader is " + targetPartition.getLeader());
            return;
        }

        // Check if the offset is valid
        if (offset >= targetPartition.getLogEndOffset()) {
            // No messages available at this offset
            ByteBuffer response = ByteBuffer.allocate(5);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0); // 0 messages
            response.flip();
            Protocol.writeFully(clientChannel, response);
            return;
        }

        // Read messages from log
        List<byte[]> messages = targetPartition.readMessages(offset, maxBytes);

        // Counted in a long: each record carries 12 bytes of framing on top of its
        // payload, so a reply full of small records is bigger than the byte budget
        // suggests - and an int total could wrap negative straight into allocate().
        long totalSize = 5L; // 1 byte for response type, 4 bytes for message count
        for (byte[] msg : messages) {
            totalSize += 12 + msg.length; // 8 bytes for offset, 4 bytes for length, plus message bytes
        }

        if (totalSize > MAX_FETCH_REPLY_BYTES) {
            Protocol.sendErrorResponse(clientChannel,
                    "Fetch reply of " + totalSize + " bytes exceeds the " + MAX_FETCH_REPLY_BYTES + " byte limit");
            return;
        }

        ByteBuffer response = ByteBuffer.allocate((int) totalSize);
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
        Protocol.writeFully(clientChannel, response);
    }

    /**
     * Forget a broker, as a missed /brokers notification would. Exists so a test can
     * put this broker in the state a metadata response has to repair.
     */
    void forgetBroker(int brokerId) {
        clusterMetadata.remove(brokerId);
    }

    /**
     * Add a broker, as a /brokers notification would. Together with
     * {@link #forgetBroker} this lets a test change what a metadata response has to
     * describe while one is being built.
     */
    void rememberBroker(BrokerInfo broker) {
        clusterMetadata.put(broker.getId(), broker);
    }

    /**
     * Look up any broker a partition assignment names that this broker does not know
     * about yet. Every metadata response has to describe every broker it names, or the
     * client is handed a leader it cannot open a connection to.
     */
    private void ensureAssignedBrokersAreKnown() {
        for (List<Partition> partitions : topics.values()) {
            for (Partition partition : partitions) {
                ensureKnown(partition.getLeader());
                for (int follower : partition.getFollowers()) {
                    ensureKnown(follower);
                }
            }
        }
    }

    /**
     * Read one broker's address out of ZooKeeper, if it is there and we did not know it.
     * A broker that has genuinely gone is simply absent: its /brokers node was ephemeral
     * and the same session ending is what removed it from the cluster.
     */
    private void ensureKnown(int brokerId) {
        if (brokerId < 0 || clusterMetadata.containsKey(brokerId)) {
            return;
        }

        try {
            String data = zkClient.readDataIfPresent("/brokers/" + brokerId);
            if (data == null || data.isEmpty()) {
                return;
            }

            int separator = data.lastIndexOf(':');
            if (separator <= 0) {
                LOGGER.warning("Broker " + brokerId + " registered an address this broker cannot read: " + data);
                return;
            }

            BrokerInfo info = new BrokerInfo(brokerId,
                    data.substring(0, separator),
                    Integer.parseInt(data.substring(separator + 1)));
            clusterMetadata.put(brokerId, info);
            LOGGER.info("Learned broker " + brokerId + " from a partition assignment: " + info);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not look up broker " + brokerId, e);
        }
    }

    /**
     * Handle metadata request from client
     */
    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        Protocol.writeFully(clientChannel, encodeMetadataResponse());
    }

    /**
     * The bytes a metadata request is answered with.
     *
     * <p>Built by encoding each entry into its own buffer and counting the entries that
     * came out, rather than by sizing one buffer from a pass over the maps and filling
     * it from a second. Those maps are changed by other threads - a broker joining, a
     * topic being loaded, a leader moving - and anything that appeared between the two
     * passes would be written into a buffer sized without it, while anything that
     * disappeared would leave a count describing an entry that is never written. The
     * counts here describe exactly what was encoded, so the reply is whole by
     * construction.
     */
    ByteBuffer encodeMetadataResponse() {
        // An assignment can name a broker this one has not heard of yet: it arrives
        // through the partition's own watch, which can beat the /brokers watch that would
        // add the broker to clusterMetadata. A client cannot route to a leader whose
        // address its metadata does not carry, and it has no other way to find out - so
        // fill the gap from ZooKeeper, the same source the assignment came from, before
        // describing either.
        ensureAssignedBrokersAreKnown();

        ByteArrayOutputStream brokers = new ByteArrayOutputStream();
        int brokerCount = 0;
        for (BrokerInfo broker : clusterMetadata.values()) {
            byte[] host = Protocol.utf8(broker.getHost());
            ByteBuffer entry = ByteBuffer.allocate(4 + 2 + host.length + 4);
            entry.putInt(broker.getId());
            entry.putShort((short) host.length);
            entry.put(host);
            entry.putInt(broker.getPort());
            entry.flip();
            brokers.write(entry.array(), 0, entry.limit());
            brokerCount++;
        }

        ByteArrayOutputStream topicData = new ByteArrayOutputStream();
        int topicCount = 0;
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            byte[] topicBytes = Protocol.utf8(entry.getKey());
            List<Partition> partitions = entry.getValue();

            // Take each follower list once, so the size and the contents come from the
            // same reading of it.
            List<int[]> followers = new ArrayList<>();
            for (Partition partition : partitions) {
                List<Integer> ids = partition.getFollowers();
                int[] copy = new int[ids.size()];
                for (int i = 0; i < copy.length; i++) {
                    copy[i] = ids.get(i);
                }
                followers.add(copy);
            }

            int size = 2 + topicBytes.length + 4;
            for (int[] copy : followers) {
                size += 12 + copy.length * 4; // id, leader, follower count, then the ids
            }

            ByteBuffer topicEntry = ByteBuffer.allocate(size);
            topicEntry.putShort((short) topicBytes.length);
            topicEntry.put(topicBytes);
            topicEntry.putInt(partitions.size());
            for (int i = 0; i < partitions.size(); i++) {
                topicEntry.putInt(partitions.get(i).getId());
                topicEntry.putInt(partitions.get(i).getLeader());
                int[] copy = followers.get(i);
                topicEntry.putInt(copy.length);
                for (int follower : copy) {
                    topicEntry.putInt(follower);
                }
            }
            topicEntry.flip();

            topicData.write(topicEntry.array(), 0, topicEntry.limit());
            topicCount++;
        }

        byte[] brokerBytes = brokers.toByteArray();
        byte[] topicBytes = topicData.toByteArray();

        ByteBuffer response = ByteBuffer.allocate(1 + 4 + brokerBytes.length + 4 + topicBytes.length);
        response.put(Protocol.METADATA_RESPONSE);
        response.putInt(brokerCount);
        response.put(brokerBytes);
        response.putInt(topicCount);
        response.put(topicBytes);
        response.flip();
        return response;
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
        String configProblem = validateTopicConfig(numPartitions, replicationFactor, clusterMetadata.size());
        if (configProblem != null) {
            Protocol.sendErrorResponse(clientChannel, configProblem);
            return;
        }

        // As controller, create the topic
        if (isController.get()) {
            if (!createTopic(topic, numPartitions, replicationFactor)) {
                Protocol.sendErrorResponse(clientChannel, "Failed to create topic " + topic);
                return;
            }

            // Send success response
            ByteBuffer response = ByteBuffer.allocate(2);
            response.put(Protocol.CREATE_TOPIC_RESPONSE);
            response.put((byte) 0); // 0 = success
            response.flip();
            Protocol.writeFully(clientChannel, response);
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

            // A forwarded create-topic is byte for byte the request the client sent.
            ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);

            // Send request to controller
            Protocol.writeFully(controllerChannel, request);

            // Read response from controller
            ByteBuffer response = ByteBuffer.allocate(2);
            Protocol.readFully(controllerChannel, response, Protocol.DEFAULT_IO_TIMEOUT_MS);
            response.flip();

            // Forward controller's response back to client
            Protocol.writeFully(clientChannel, response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller");
        }
    }

    /**
     * Create a new topic with the specified configuration.
     *
     * @return whether the topic now exists on this broker
     */
    private boolean createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            LOGGER.warning("Only the controller can create topics");
            return false;
        }

        // Topic/partition directories are created lazily by Partition itself,
        // and only for the partitions this broker actually replicates.
        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        String topicPath = "/topics/" + topic;

        List<Partition> partitions = new ArrayList<>();
        boolean createdTopicNode = false;

        try {
            // Create topic in ZooKeeper
            if (!zkClient.exists(topicPath)) {
                zkClient.createPersistentNode(topicPath, "");
                createdTopicNode = true;
                zkClient.createPersistentNode(topicPath + "/partitions", "");
            }

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

                // Arm the watch even though we just wrote the assignment ourselves: this
                // broker may stop being the controller, and then it is the one that has
                // to be told about somebody else's decision.
                watchPartitionAssignment(topic, partition);
            }

            // Add topic to broker's metadata
            topics.put(topic, partitions);

            // Notify all brokers to load the topic
            for (int brokerId : brokerIds) {
                if (brokerId != this.brokerId) {
                    notifyBrokerForTopicCreation(brokerId, topic);
                }
            }

            return true;
        } catch (Exception e) {
            // The client is waiting for an answer, and the one answer it must not get is
            // "created". Undo what this call managed before it failed: the partitions are
            // holding open file handles and armed watches, and the ZooKeeper nodes would
            // make the next broker to load this topic believe in one that was never
            // finished. WARNING rather than SEVERE because this is a reported outcome -
            // the client is told - not a broker failure nobody sees.
            LOGGER.log(Level.WARNING, "Failed to create topic " + topic + "; undoing it", e);
            for (Partition partition : partitions) {
                partition.close();
            }
            removeTopicNodes(topicPath, createdTopicNode);
            return false;
        }
    }

    /**
     * Take back the ZooKeeper nodes a failed create-topic left behind: the partitions
     * this call wrote, and the topic node itself when this call is what created it.
     */
    private void removeTopicNodes(String topicPath, boolean createdTopicNode) {
        try {
            for (String partitionId : zkClient.getChildren(topicPath + "/partitions")) {
                deleteNodeQuietly(topicPath + "/partitions/" + partitionId);
            }
            if (createdTopicNode) {
                deleteNodeQuietly(topicPath + "/partitions");
                deleteNodeQuietly(topicPath);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not remove the ZooKeeper nodes of a failed topic creation", e);
        }
    }

    private void deleteNodeQuietly(String path) {
        try {
            zkClient.deleteNode(path);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not remove " + path, e);
        }
    }
}