package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Controller election must be a single race that exactly one broker wins, without
 * the losers reporting it as an error.
 *
 * <p>The brokers are started at the same time on purpose: every one of them wants
 * the same node, so this is where the "check if it exists, then create it" version
 * of the election used to have two brokers delete each other's controller or die on
 * a NodeExists exception they logged as SEVERE.
 */
class ControllerElectionTest {

    private static final int[] BROKER_IDS = {1, 2, 3};
    private static final long TIMEOUT = 60_000;

    @TempDir
    static Path tempDir;

    private static final Map<Integer, SimpleKafkaBroker> brokers = new LinkedHashMap<>();
    private static NIOServerCnxnFactory zooKeeperFactory;
    private static ZooKeeperServer zooKeeperServer;
    private static int zooKeeperPort;

    @BeforeAll
    static void startZooKeeper() throws Exception {
        zooKeeperPort = freePort();
        Path zooKeeperData = Files.createDirectories(tempDir.resolve("zookeeper"));
        zooKeeperServer = new ZooKeeperServer(zooKeeperData.toFile(), zooKeeperData.toFile(), 2000);
        zooKeeperFactory = new NIOServerCnxnFactory();
        zooKeeperFactory.configure(new InetSocketAddress("127.0.0.1", zooKeeperPort), 100);
        zooKeeperFactory.startup(zooKeeperServer);
    }

    @AfterAll
    static void stopZooKeeper() {
        for (SimpleKafkaBroker broker : new ArrayList<>(brokers.values())) {
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
    }

    @Test
    void threeBrokersRacingAtOnceElectExactlyOneControllerWithoutReportingAFailure() throws Exception {
        LogCapture capture = LogCapture.attach();
        try (ZooKeeper testClient = connectAsTestClient()) {
            startBrokersConcurrently();

            awaitTrue("a controller to be elected", TIMEOUT, () -> {
                String controller = readController(testClient);
                return controller != null && !controller.trim().isEmpty();
            });

            int controllerId = Integer.parseInt(readController(testClient));
            assertTrue(List.of(1, 2, 3).contains(controllerId),
                    "controller " + controllerId + " is not one of " + List.of(1, 2, 3));

            // Losing the race is the normal outcome for every broker but one, so it
            // must never be reported as an election failure.
            assertEquals(0, capture.count(Level.SEVERE, "Controller election failed"),
                    "losing the controller race must not be logged as an error");
            assertEquals(1, capture.countContaining("This broker is now the active controller"),
                    "exactly one broker may take controllership");
            assertEquals(0, capture.countContaining("Controller node exists but has no data"),
                    "an empty controller node is a leftover placeholder, not a surprise");
        } finally {
            capture.detach();
        }
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Release all three brokers from a barrier so their connections and their
     * election attempts overlap instead of politely taking turns.
     */
    private static void startBrokersConcurrently() throws Exception {
        Map<Integer, Integer> ports = new LinkedHashMap<>();
        for (int brokerId : BROKER_IDS) {
            ports.put(brokerId, freePort());
        }

        CyclicBarrier barrier = new CyclicBarrier(BROKER_IDS.length);
        Map<Integer, Exception> failures = new ConcurrentHashMap<>();
        List<Thread> threads = new ArrayList<>();

        for (int brokerId : BROKER_IDS) {
            int port = ports.get(brokerId);
            Thread thread = new Thread(() -> {
                try {
                    barrier.await(20, TimeUnit.SECONDS);
                    SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, "127.0.0.1", port, zooKeeperPort);
                    broker.start();
                    synchronized (brokers) {
                        brokers.put(brokerId, broker);
                    }
                } catch (Exception e) {
                    failures.put(brokerId, e);
                }
            }, "start-broker-" + brokerId);
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(60));
        }

        if (!failures.isEmpty()) {
            fail("Brokers failed to start: " + failures);
        }
        assertEquals(BROKER_IDS.length, brokers.size(), "every broker must have started");
    }

    private static String readController(ZooKeeper testClient) throws Exception {
        try {
            byte[] data = testClient.getData("/controller", false, null);
            return data == null ? "" : new String(data, StandardCharsets.UTF_8);
        } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
            return null;
        }
    }

    private static ZooKeeper connectAsTestClient() throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:" + zooKeeperPort, 15_000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (!connected.await(15, TimeUnit.SECONDS)) {
            zooKeeper.close();
            fail("The test's ZooKeeper client did not connect");
        }
        return zooKeeper;
    }

    private static void awaitTrue(String what, long timeoutMillis, Condition condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.isMet()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for " + what);
    }

    @FunctionalInterface
    private interface Condition {
        boolean isMet() throws Exception;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    /**
     * Collects the broker logs so a test can assert on what was actually reported
     * rather than scraping stdout.
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
            if (record != null && record.getLevel().intValue() >= Level.ALL.intValue()) {
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
}
