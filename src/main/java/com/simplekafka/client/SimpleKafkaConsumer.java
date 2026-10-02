package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.simplekafka.broker.Protocol;

/**
 * Example consumer for Build Your Own Kafka
 */
public class SimpleKafkaConsumer {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());
    private static final int MAX_BYTES = 1024 * 1024; // 1MB max fetch size
    private static final int POLL_INTERVAL_MS = 100;
    /** How many polls in a row may fail before this consumer gives up. */
    private static final int MAX_CONSECUTIVE_FAILURES = 10;
    /** Ceiling on the wait between retries. */
    private static final long MAX_BACKOFF_MS = 5_000;
    
    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    /** Read by the consuming thread and by whoever calls seek; volatile for both. */
    private volatile long currentOffset;
    private final AtomicBoolean running;
    private Thread consumerThread;
    
    /**
     * Create a SimpleKafka consumer
     * @param bootstrapBroker Hostname of a broker to connect to
     * @param bootstrapPort Port of the broker to connect to
     * @param topic Topic to consume from
     * @param partition Partition to consume from
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this(bootstrapBroker, bootstrapPort, topic, partition, 0);
    }
    
    /**
     * Create a SimpleKafka consumer with a specific starting offset
     * @param bootstrapBroker Hostname of a broker to connect to
     * @param bootstrapPort Port of the broker to connect to
     * @param topic Topic to consume from
     * @param partition Partition to consume from
     * @param startOffset Offset to start consuming from
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition, long startOffset) {
        this(new SimpleKafkaClient(bootstrapBroker, bootstrapPort), topic, partition, startOffset);
    }

    /**
     * Consume through a supplied client. Exists so a test can make a poll fail and watch
     * what the consuming loop does about it.
     */
    SimpleKafkaConsumer(SimpleKafkaClient client, String topic, int partition, long startOffset) {
        this.client = client;
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startOffset;
        this.running = new AtomicBoolean(false);
    }
    
    /**
     * Initialize the consumer
     */
    public void initialize() throws IOException {
        client.initialize();
        
        // Check if topic exists
        if (client.getTopicMetadata(topic) == null) {
            throw new IOException("Topic does not exist: " + topic);
        }
    }
    
    /**
     * Set the offset to start consuming from
     */
    public void seek(long offset) {
        this.currentOffset = offset;
    }
    
    /**
     * Poll for new messages (single poll)
     */
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = new ArrayList<>();
        for (Protocol.Record record : pollRecords()) {
            messages.add(record.getPayload());
        }
        return messages;
    }

    /**
     * Poll, keeping each record's own offset.
     *
     * <p>The next offset comes from the records rather than from how many there are. A
     * reader can start earlier than the offset it asked for - the partition's index can
     * fall back to an earlier point - and counting forwards from the request would then
     * leave the cursor past messages that were never handed over: they would be skipped,
     * silently, and the count would still look right.
     */
    private List<Protocol.Record> pollRecords() throws IOException {
        List<Protocol.Record> records = client.fetchRecords(topic, partition, currentOffset, MAX_BYTES);
        if (!records.isEmpty()) {
            currentOffset = records.get(records.size() - 1).getOffset() + 1;
        }
        return records;
    }
    
    /**
     * Start consuming messages in a loop
     */
    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                int consecutiveFailures = 0;

                while (running.get()) {
                    try {
                        List<Protocol.Record> records = pollRecords();
                        consecutiveFailures = 0;

                        // Each record is handed over with the offset it occupies, taken
                        // from the record itself. Working it out from the position in the
                        // batch was both wrong when the broker started earlier and
                        // quadratic, since List.indexOf compares by reference.
                        for (Protocol.Record record : records) {
                            handler.handle(record.getPayload(), record.getOffset());
                        }

                        // If no messages, wait a bit before polling again
                        if (records.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        // A broker restarting or a leader moving is an ordinary event for a
                        // consumer, not a reason to stop for good. This used to leave the
                        // loop on the first failure, so an application was left with a
                        // consumer that had quietly stopped consuming.
                        consecutiveFailures++;
                        if (consecutiveFailures > MAX_CONSECUTIVE_FAILURES) {
                            LOGGER.log(Level.SEVERE, "Giving up after " + consecutiveFailures +
                                    " consecutive failed polls", e);
                            break;
                        }

                        long backoff = Math.min((long) POLL_INTERVAL_MS * consecutiveFailures, MAX_BACKOFF_MS);
                        LOGGER.log(Level.WARNING, "Poll failed (" + consecutiveFailures + " in a row); retrying in "
                                + backoff + "ms: " + e.getMessage());
                        try {
                            Thread.sleep(backoff);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }

                running.set(false);
            });
            
            consumerThread.setDaemon(true);
            consumerThread.start();
            
            LOGGER.info("Started consuming from topic: " + topic + ", partition: " + partition);
        }
    }
    
    /**
     * Stop consuming messages
     */
    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            if (consumerThread != null) {
                try {
                    consumerThread.interrupt();
                    consumerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            LOGGER.info("Stopped consuming from topic: " + topic + ", partition: " + partition);
        }
    }
    
    /**
     * Get the current offset
     */
    public long getCurrentOffset() {
        return currentOffset;
    }
    
    /**
     * Check if consumer is running
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Close the consumer
     */
    public void close() {
        stopConsuming();
    }
    
    /**
     * Interface for handling consumed messages
     */
    public interface MessageHandler {
        void handle(byte[] message, long offset);
    }
    
    /** Command line option for the offset to start consuming from. */
    private static final String OFFSET_OPTION = "--offset";

    /** The command line, once the starting offset has been pulled out of it. */
    static final class CommandLine {
        final String broker;
        final int port;
        final String topic;
        final int partition;
        final long startOffset;

        CommandLine(String broker, int port, String topic, int partition, long startOffset) {
            this.broker = broker;
            this.port = port;
            this.topic = topic;
            this.partition = partition;
            this.startOffset = startOffset;
        }
    }

    /**
     * Read the command line.
     *
     * <p>Four positional arguments are required: broker, port, topic and partition. The
     * offset to start from is optional and may be given either as a fifth positional
     * argument or as {@code --offset <n>} / {@code --offset=<n>} anywhere on the command
     * line; with neither, consumption starts at 0.
     *
     * @throws IllegalArgumentException if the command line does not make sense
     */
    static CommandLine parseArgs(String[] args) {
        List<String> positional = new ArrayList<>();
        long startOffset = 0;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (OFFSET_OPTION.equals(arg)) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException(OFFSET_OPTION + " needs a value");
                }
                startOffset = parseOffset(args[++i]);
            } else if (arg.startsWith(OFFSET_OPTION + "=")) {
                startOffset = parseOffset(arg.substring(OFFSET_OPTION.length() + 1));
            } else if (arg.startsWith("--")) {
                throw new IllegalArgumentException("Unknown option: " + arg);
            } else {
                positional.add(arg);
            }
        }

        if (positional.size() < 4) {
            throw new IllegalArgumentException(
                    "expected <broker> <port> <topic> <partition>, got " + positional.size() + " argument(s)");
        }
        if (positional.size() > 4) {
            // Whatever sits past the four required arguments is the offset.
            startOffset = parseOffset(positional.get(4));
        }
        if (positional.size() > 5) {
            throw new IllegalArgumentException("Unexpected argument: " + positional.get(5));
        }

        return new CommandLine(positional.get(0), parseNumber(positional.get(1), "port"),
                positional.get(2), parseNumber(positional.get(3), "partition"), startOffset);
    }

    private static int parseNumber(String value, String name) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer: " + value);
        }
    }

    private static long parseOffset(String value) {
        long offset;
        try {
            offset = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("offset must be an integer: " + value);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative: " + offset);
        }
        return offset;
    }

    private static void printUsage() {
        System.out.println("Usage: SimpleKafkaConsumer <broker> <port> <topic> <partition> [offset]");
        System.out.println("       The offset may also be given as --offset <n> or --offset=<n>; it defaults to 0.");
    }

    /**
     * Main method for demonstration
     */
    public static void main(String[] args) {
        CommandLine commandLine;
        try {
            commandLine = parseArgs(args);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        try {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(commandLine.broker, commandLine.port,
                    commandLine.topic, commandLine.partition, commandLine.startOffset);
            consumer.initialize();

            System.out.println("Consumer initialized. Starting consumption from offset "
                    + commandLine.startOffset + "...");
            
            // Consume messages and print them
            consumer.startConsuming((message, offset) -> {
                String messageStr = new String(message, StandardCharsets.UTF_8);
                System.out.println("Received message at offset " + offset + ": " + messageStr);
            });
            
            System.out.println("Consumer started. Press enter to stop.");
            System.in.read();
            
            consumer.close();
            System.out.println("Consumer stopped");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Consumer error", e);
        }
    }
}