package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * What happens when ZooKeeper is not there.
 *
 * <p>Connecting waited on a latch that only a successful connection releases, with no
 * bound. A broker pointed at an address nothing answers on would bind its port, then
 * block for ever in registration - listening, accepting nothing, and logging nothing to
 * say why. "It never started" is a far better answer than that.
 */
class ZookeeperClientTest {

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

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
