package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What happens when ZooKeeper is not there.
 *
 * <p>Connecting waited on a latch that only a successful connection releases, with no
 * bound. A broker pointed at an address nothing answers on would bind its port, then
 * block for ever in registration - listening, accepting nothing, and logging nothing to
 * say why. "It never started" is a far better answer than that.
 */
class ZookeeperClientTest {

    @TempDir
    static Path tempDir;

    @Test
    void connectingToAnAddressNothingAnswersOnGivesUp() throws Exception {
        int deadPort = freePort();
        ZookeeperClient client = new ZookeeperClient("127.0.0.1", deadPort, 2_000);

        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            long start = System.currentTimeMillis();
            assertThrows(IOException.class, () -> client.connect());
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed < 10_000, "connect took " + elapsed + "ms to give up");
        });
    }

    @Test
    void aBrokerThatCannotReachZooKeeperRefusesToStart() throws Exception {
        int deadPort = freePort();
        int port = freePort();
        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "127.0.0.1", port, deadPort);

        assertThrows(IOException.class, broker::start,
                "a broker that cannot join the cluster must say so rather than listen and do nothing");

        // And it must release the port it took before giving up, or the next attempt to
        // start a broker there fails for a reason that has nothing to do with ZooKeeper.
        try (ServerSocket reusable = new ServerSocket()) {
            reusable.setReuseAddress(true);
            reusable.bind(new InetSocketAddress("127.0.0.1", port));
        }
    }

    /**
     * A broker whose ZooKeeper has gone must still be stoppable.
     *
     * <p>This does not fail without the timeout on the close - the deadlock that made a
     * broker ignore SIGTERM for ever was in the session-expiry handler, and that was fixed
     * separately. It is here because stop() runs on the shutdown hook, where anything that
     * waits is a process that cannot be stopped, and ZooKeeper's close() waits on that
     * client's own reconnect loop. The scenario is real even though this instance of it
     * no longer reproduces.
     */
    @Test
    void aBrokerWhoseZooKeeperIsGoneStillStops() throws Exception {
        int zooKeeperPort = freePort();
        Path zooKeeperData = Files.createDirectories(tempDir.resolve("zookeeper"));
        ZooKeeperServer zooKeeperServer =
                new ZooKeeperServer(zooKeeperData.toFile(), zooKeeperData.toFile(), 2000);
        NIOServerCnxnFactory zooKeeperFactory = new NIOServerCnxnFactory();
        zooKeeperFactory.configure(new InetSocketAddress("127.0.0.1", zooKeeperPort), 100);
        zooKeeperFactory.startup(zooKeeperServer);

        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "127.0.0.1", freePort(), zooKeeperPort);
        broker.start();

        // Take ZooKeeper away underneath it, so the client is left trying to reach a
        // server that is not there.
        zooKeeperFactory.shutdown();
        zooKeeperServer.shutdown();
        Thread.sleep(500);

        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(20), broker::stop);
    }

    /**
     * A watch that cannot reach ZooKeeper is a transport problem, not a broker failure.
     *
     * <p>This client rides out a lost or expired session by itself, so a blip would
     * otherwise paint a healthy broker's log red - and anything at SEVERE is read as a
     * failure by smoke.sh, which means a blip could fail a run against a cluster that did
     * nothing wrong. Anything that is not a session problem is still an error.
     */
    @Test
    void aSessionProblemIsNotReportedAsABrokerError() {
        List<String> severe = Collections.synchronizedList(new ArrayList<>());
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (Level.SEVERE.equals(record.getLevel()) && record.getMessage() != null) {
                    severe.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        capture.setLevel(Level.ALL);
        Logger.getLogger("com.simplekafka.broker").addHandler(capture);

        try {
            ZookeeperClient.logWatchFailure("Watching /brokers failed",
                    new KeeperException.SessionExpiredException());
            ZookeeperClient.logWatchFailure("Watching /brokers failed",
                    new KeeperException.ConnectionLossException());

            assertEquals(0, severe.size(),
                    "a session that has gone is something this client recovers from, not a broker error");

            ZookeeperClient.logWatchFailure("Watching /brokers failed", new RuntimeException("genuinely wrong"));

            assertEquals(1, severe.size(),
                    "anything that is not a session problem must still be reported as an error");
        } finally {
            Logger.getLogger("com.simplekafka.broker").removeHandler(capture);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
