package com.simplekafka.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;

/**
 * What the consuming loop does when a poll fails.
 *
 * <p>A broker restarting, or a leader moving to another replica, is an ordinary event for
 * a consumer. Leaving the loop on the first failure leaves the application holding a
 * consumer that has quietly stopped consuming, with nothing but a log line to say so.
 */
class SimpleKafkaConsumerTest {

    /**
     * A client whose polls fail a set number of times and then start answering. The
     * address it is constructed with is never used, because fetching is overridden.
     */
    private static final class FlakyClient extends SimpleKafkaClient {
        private final AtomicInteger polls = new AtomicInteger();
        private final int failures;

        FlakyClient(int failures) {
            super("127.0.0.1", 1);
            this.failures = failures;
        }

        @Override
        public List<Protocol.Record> fetchRecords(String topic, int partition, long offset, int maxBytes)
                throws IOException {
            int attempt = polls.incrementAndGet();
            if (attempt <= failures) {
                throw new IOException("transient failure " + attempt);
            }
            return List.of(new Protocol.Record(offset, ("m" + attempt).getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void theLoopKeepsPollingAfterAPollFails() throws Exception {
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(new FlakyClient(3), "t", 0, 0);

        CountDownLatch handled = new CountDownLatch(1);
        consumer.startConsuming((message, offset) -> handled.countDown());

        try {
            assertTrue(handled.await(20, TimeUnit.SECONDS),
                    "a consumer must survive polls that failed and carry on consuming");
        } finally {
            consumer.close();
        }
    }

    @Test
    void theLoopGivesUpRatherThanRetryingForEver() throws Exception {
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(new FlakyClient(Integer.MAX_VALUE), "t", 0, 0);
        consumer.startConsuming((message, offset) -> { });

        try {
            long deadline = System.currentTimeMillis() + 60_000;
            while (consumer.isRunning() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertFalse(consumer.isRunning(),
                    "a consumer whose polls never succeed must stop rather than retry for ever");
        } finally {
            consumer.close();
        }
    }
}
