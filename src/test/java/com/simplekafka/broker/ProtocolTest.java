package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

/**
 * The wire helpers, on their own. Every length on this wire counts bytes, so these
 * check that a name's encoded length - not its character count - is what sizes a
 * buffer and what goes into a length field.
 */
class ProtocolTest {

    /**
     * A name with non-ASCII characters is longer in bytes than in chars. Sizing the
     * buffer or the length prefix from the char count overflows the buffer and tells
     * the reader the wrong span.
     */
    @Test
    void produceRequestIsSizedInBytesNotChars() {
        // "café" is 4 chars and 5 bytes; the emoji is 2 chars (a surrogate pair) and 4 bytes.
        for (String topic : List.of("café", "😀", "plain", "aéb")) {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            byte[] message = {1, 2, 3};

            ByteBuffer encoded = Protocol.encodeProduceRequest(topic, 0, message);

            assertEquals(11 + topicBytes.length + message.length, encoded.remaining(),
                    "the request must be sized in the topic's bytes: " + topic);

            assertEquals(Protocol.PRODUCE, encoded.get());
            assertEquals(topicBytes.length, encoded.getShort() & 0xFFFF,
                    "the length prefix must count bytes, not chars: " + topic);

            byte[] written = new byte[topicBytes.length];
            encoded.get(written);
            assertEquals(topic, new String(written, StandardCharsets.UTF_8),
                    "the topic must survive a round trip: " + topic);
        }
    }

    /**
     * A 2-byte length field is unsigned. Read with a sign-extending {@code getShort()},
     * anything from 32768 up arrives negative and is handed to an array allocation.
     */
    @Test
    void anUnsignedLengthOf40000DecodesAsPositive() {
        byte[] text = new byte[40000];
        Arrays.fill(text, (byte) 'x');

        ByteBuffer frame = ByteBuffer.allocate(3 + text.length);
        frame.put(Protocol.ERROR_RESPONSE);
        frame.putShort((short) text.length); // 40000 does not fit in a signed short
        frame.put(text);
        frame.flip();

        Protocol.ProduceResult result = Protocol.decodeProduceResponse(frame);

        assertFalse(result.isSuccess());
        assertEquals(40000, result.getError().length(),
                "a length of 40000 must not decode as a negative array size");
    }

    /**
     * A peer that stops reading must not be able to hang the writer for as long as the
     * operating system allows: on a blocking channel {@code write} blocks inside the
     * socket rather than returning zero, so the deadline has to be enforced by not
     * blocking in the first place.
     */
    @Test
    void writeFullyGivesUpAtItsDeadlineWhenThePeerStopsReading() throws Exception {
        try (ServerSocketChannel listener = ServerSocketChannel.open()) {
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            int port = ((InetSocketAddress) listener.getLocalAddress()).getPort();

            try (SocketChannel sender = SocketChannel.open()) {
                sender.connect(new InetSocketAddress("127.0.0.1", port));

                try (SocketChannel peer = listener.accept()) {
                    // Far more than the socket buffers can hold, and the peer never reads.
                    ByteBuffer tooBigToBuffer = ByteBuffer.allocate(32 * 1024 * 1024);

                    // The plain overload's deadline is DEFAULT_IO_TIMEOUT_MS (10s), so a
                    // writer that honours it finishes here and one that blocks does not.
                    Assertions.assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
                        long start = System.currentTimeMillis();
                        assertThrows(IOException.class,
                                () -> Protocol.writeFully(sender, tooBigToBuffer),
                                "the writer must give up at its deadline instead of blocking");
                        long elapsed = System.currentTimeMillis() - start;
                        assertTrue(elapsed < 15_000, "gave up after " + elapsed + "ms");
                    });
                }
            }
        }
    }

    @Test
    void writeFullyStillWritesEverythingWhenThePeerKeepsUp() throws Exception {
        try (ServerSocketChannel listener = ServerSocketChannel.open()) {
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            int port = ((InetSocketAddress) listener.getLocalAddress()).getPort();

            try (SocketChannel sender = SocketChannel.open()) {
                sender.connect(new InetSocketAddress("127.0.0.1", port));

                try (SocketChannel peer = listener.accept()) {
                    byte[] payload = new byte[200_000];
                    Arrays.fill(payload, (byte) 'z');
                    ByteBuffer out = ByteBuffer.wrap(payload);

                    // No timeout argument: the plain overload must still write it all.
                    Protocol.writeFully(sender, out);
                    assertFalse(out.hasRemaining(), "every byte must have been written");
                    assertTrue(sender.isBlocking(), "the channel's blocking mode must be restored");
                }
            }
        }
    }
}
