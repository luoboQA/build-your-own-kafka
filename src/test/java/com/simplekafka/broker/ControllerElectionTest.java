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

    /**
     * The controller has to notice that it stopped being one.
     *
     * <p>isController was only cleared when this broker left /brokers, and the winner
     * never armed a watch on the node it owned - only the losers did. So a controller
     * whose node went away, which is what an expired session does to it, went on acting
     * as controller: two brokers writing assignments and both answering create-topic as
     * the authority.
     */
    @Test
    void aControllerThatLosesItsNodeStopsBelievingItIsController() throws Exception {
        // Take the cluster apart first: brokers left over from the other test would hold
        // /controller and make this one a loser rather than the winner.
        stopAllBrokers();

        // Started alone on purpose. In a race, a broker that loses an attempt arms a
        // watch on /controller as part of losing, and the winner can pick one up that
        // way by accident - which is not a guarantee, just a coincidence of timing. With
        // nobody to race, the only watch the winner can have is the one it arms itself.
        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "127.0.0.1", freePort(), zooKeeperPort);
        broker.start();
        synchronized (brokers) {
            brokers.put(1, broker);
        }

        try (ZooKeeper testClient = connectAsTestClient()) {
            awaitTrue("the lone broker to take controllership", TIMEOUT,
                    () -> "1".equals(readController(testClient)));
            assertTrue(broker.isController(),
                    "the broker named by /controller must believe it is the controller");

            // Hand the node to nobody in particular behind its back, which is what the
            // broker's own session ending would do to it.
            testClient.setData("/controller", "99".getBytes(StandardCharsets.UTF_8), -1);

            awaitTrue("the controller to notice it no longer holds the node", TIMEOUT,
                    () -> !broker.isController());
        } finally {
            stopAllBrokers();
        }
    }

    /**
     * A scheduled election retry must not run once the broker has stopped.
     *
     * <p>electController falls back to scheduling itself again a second or two later, and
     * that thread never asked whether the broker was still running. It woke up after stop()
     * had closed ZooKeeper, failed with SessionExpired, logged it at SEVERE and scheduled
     * another one - for ever, every couple of seconds, in a process that was shutting down
     * and had nothing left to elect. That noise is what a test run's teardown looked like.
     */
    @Test
    void aStoppedBrokerDoesNotRunAScheduledElection() throws Exception {
        // Run alone, like the test above: leftover brokers would hold /controller.
        stopAllBrokers();

        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "127.0.0.1", freePort(), zooKeeperPort);
        broker.start();
        broker.stop();

        LogCapture capture = LogCapture.attach();
        try {
            // Exactly what the retry thread does when its sleep ends.
            broker.electController();

            assertEquals(0, capture.count(Level.SEVERE, "Controller election failed"),
                    "a broker that has stopped has nothing to elect and must not report failing to");
        } finally {
            capture.detach();
        }
    }

    private static void stopAllBrokers() {
        for (SimpleKafkaBroker running : new ArrayList<>(brokers.values())) {
            try {
                running.stop();
            } catch (Exception ignored) {
                // best effort
            }
        }
        brokers.clear();
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
