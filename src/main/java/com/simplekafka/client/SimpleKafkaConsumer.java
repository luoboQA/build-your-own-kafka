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
    
    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private long currentOffset;
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
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
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
                try {
                    while (running.get()) {
                        List<Protocol.Record> records = pollRecords();

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
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                    }
                    running.set(false);
                }
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
    
    /**
     * Main method for demonstration
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: SimpleKafkaConsumer <broker> <port> <topic> <partition>");
            System.exit(1);
        }
        
        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);
        
        try {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(broker, port, topic, partition);
            consumer.initialize();
            
            System.out.println("Consumer initialized. Starting consumption...");
            
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