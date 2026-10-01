package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Offset handling on a single replica. The leader owns the offsets, so a replica
 * must write where it is told instead of wherever its own counter happens to be.
 */
class PartitionTest {

    @TempDir
    Path tempDir;

    private Partition newPartition(String name) {
        return new Partition(0, 1, List.of(2), tempDir.resolve(name).toString());
    }

    private static byte[] message(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] message) {
        return new String(message, StandardCharsets.UTF_8);
    }

    private static List<String> readAll(Partition partition) {
        List<String> messages = new ArrayList<>();
        for (Protocol.Record record : partition.readMessages(0, 16 * 1024 * 1024)) {
            messages.add(text(record.getPayload()));
        }
        return messages;
    }

    @Test
    void appendAssignsSequentialOffsetsFromTheLogEndOffset() {
        Partition partition = newPartition("leader");

        assertEquals(0, partition.append(message("a")));
        assertEquals(1, partition.append(message("b")));
        assertEquals(2, partition.append(message("c")));
        assertEquals(3, partition.getLogEndOffset());
        assertEquals(List.of("a", "b", "c"), readAll(partition));
    }

    @Test
    void followerWritesAtTheLeadersOffsetInsteadOfPickingItsOwn() {
        Partition leader = newPartition("leader");
        leader.append(message("a"));
        leader.append(message("b"));

        Partition follower = newPartition("follower");

        // The leader says "b" belongs at offset 1, but this replica has not seen
        // offset 0 yet. A plain append() would have stored "b" at offset 0 here,
        // silently shifting every later message by one.
        assertEquals(-1, follower.appendAt(1, message("b")));
        assertEquals(0, follower.getLogEndOffset());
        assertEquals(List.of(), readAll(follower));

        // Once the missing message arrives, both land at the offsets the leader chose.
        assertEquals(0, follower.appendAt(0, message("a")));
        assertEquals(1, follower.appendAt(1, message("b")));

        assertEquals(List.of("a", "b"), readAll(follower));
        assertEquals(2, follower.getLogEndOffset());
    }

    @Test
    void appendAtRefusesAnOffsetAheadOfTheLogEndOffset() {
        Partition partition = newPartition("follower");
        partition.append(message("a"));

        assertEquals(-1, partition.appendAt(5, message("e")));

        assertEquals(1, partition.getLogEndOffset());
        assertEquals(List.of("a"), readAll(partition));
    }

    @Test
    void appendAtTreatsAnAlreadyReplicatedMessageAsSuccess() {
        Partition partition = newPartition("follower");
        partition.append(message("a"));

        assertEquals(0, partition.appendAt(0, message("a")));

        assertEquals(1, partition.getLogEndOffset());
        assertEquals(List.of("a"), readAll(partition));
    }

    @Test
    void appendAtTruncatesDivergedSuffixAndRewritesTheLeadersCopy() {
        Partition partition = newPartition("follower");
        partition.append(message("a"));
        partition.append(message("b"));
        partition.append(message("c"));
        assertEquals(3, partition.getLogEndOffset());

        // The leader says offset 1 holds "B": our "b" should never have been there,
        // and neither should "c" which was written after it.
        assertEquals(1, partition.appendAt(1, message("B")));

        assertEquals(2, partition.getLogEndOffset());
        assertEquals(List.of("a", "B"), readAll(partition));

        // The leader's next message continues contiguously from the new end.
        assertEquals(2, partition.appendAt(2, message("c2")));
        assertEquals(List.of("a", "B", "c2"), readAll(partition));
        assertEquals(3, partition.getLogEndOffset());
    }

    @Test
    void truncatedLogStaysTruncatedAcrossRestart() {
        Partition partition = newPartition("persisted");
        partition.append(message("a"));
        partition.append(message("b"));
        partition.append(message("c"));
        assertEquals(1, partition.appendAt(1, message("B")));
        partition.close();

        Partition reopened = new Partition(0, 1, List.of(2), tempDir.resolve("persisted").toString());
        try {
            assertEquals(List.of("a", "B"), readAll(reopened));
            assertEquals(2, reopened.getLogEndOffset());

            // The offset cursor is still the leader's, not the replica's.
            assertEquals(2, reopened.appendAt(2, message("c2")));
            assertEquals(List.of("a", "B", "c2"), readAll(reopened));
        } finally {
            reopened.close();
        }
    }

    @Test
    void appendAtTruncatesAcrossSegmentBoundaries() {
        // Roll the log over into a second segment, then diverge inside the first one.
        Partition partition = newPartition("segmented");
        int payloadSize = 200_000;
        for (int i = 0; i < 7; i++) {
            assertEquals(i, partition.append(payload(i, payloadSize)));
        }
        assertEquals(7, partition.getLogEndOffset());
        assertEquals(7, readAll(partition).size());

        // The leader disagrees about offset 3: everything from there on must go,
        // including the messages that already live in the second segment.
        byte[] leadersCopy = payload(99, payloadSize);
        assertEquals(3, partition.appendAt(3, leadersCopy));

        List<String> remaining = readAll(partition);
        assertEquals(4, remaining.size());
        assertEquals(4, partition.getLogEndOffset());
        for (int i = 0; i < 3; i++) {
            assertTrue(remaining.get(i).startsWith(prefix(i)));
        }
        assertEquals(text(leadersCopy), remaining.get(3));

        // Contiguity holds after the truncation.
        assertEquals(4, partition.appendAt(4, payload(4, payloadSize)));
        assertEquals(5, partition.getLogEndOffset());
        assertEquals(5, readAll(partition).size());
    }

    @Test
    void aBrokerOutsideTheReplicaSetKeepsTheAssignmentButNoLog() throws Exception {
        Path assignmentOnly = tempDir.resolve("assignment-only");
        Partition partition = new Partition(0, 1, List.of(2), assignmentOnly.toString(), false);

        // The assignment survives so metadata requests and write forwarding still work.
        assertFalse(partition.isLocalReplica());
        assertEquals(1, partition.getLeader());
        assertEquals(List.of(2), partition.getFollowers());

        // Nothing may be written: an empty log here would put this broker at
        // offset 0 while the real replicas already hold data.
        assertEquals(-1, partition.append(message("a")));
        assertEquals(-2, partition.appendAt(0, message("a")));
        assertEquals(0, partition.getLogEndOffset());
        assertEquals(List.of(), readAll(partition));

        partition.close();
        assertFalse(Files.exists(assignmentOnly), "a non-replica must not create a partition directory");
    }

    /**
     * A partition whose log could not be opened has no channel to append to. Reporting
     * that is the only safe answer: an NPE thrown at the caller says nothing about what
     * went wrong, and a partition that guessed an offset would be worse.
     */
    @Test
    void appendFailsInsteadOfThrowingWhenTheLogCouldNotBeInitialized() throws Exception {
        Path dir = tempDir.resolve("uninitializable");
        Files.createDirectories(dir);
        // Ends in .log, so initialize() treats it as a segment and reads the base offset
        // out of the name - which is not a number, so it gives up before opening anything.
        Files.write(dir.resolve("not-a-number.log"), new byte[0]);

        Partition partition = new Partition(0, 1, List.of(2), dir.toString());
        try {
            assertEquals(-1, partition.append(message("a")),
                    "a partition with no open log must report the failure, not throw");
            assertEquals(0, partition.getLogEndOffset());
        } finally {
            partition.close();
        }
    }

    /**
     * The index is only as trustworthy as the disk it is on. Reading its last entry
     * assumes there is one; an index holding a partial entry would otherwise seek to a
     * negative file position.
     */
    @Test
    void anUndersizedIndexDoesNotBreakReads() throws Exception {
        Partition partition = newPartition("torn-index");
        partition.append(message("a"));
        partition.append(message("b"));

        Path index = tempDir.resolve("torn-index/00000000000000000000.index");
        try (FileChannel channel = FileChannel.open(index, StandardOpenOption.WRITE)) {
            channel.truncate(8); // half an entry: no complete index entry at all
        }

        try {
            assertEquals(List.of("a", "b"), readAll(partition),
                    "a torn index must fall back to reading the log from the start");
        } finally {
            partition.close();
        }
    }

    /**
     * A record's length is read straight off the disk and then used to size an
     * allocation. Its siblings refuse a length that runs past the end of the segment;
     * this path has to as well, or a corrupt one reaches the allocator - negative
     * throws, huge exhausts the heap.
     */
    @Test
    void aCorruptLengthInTheLogEndsTheReadInsteadOfBeingAllocated() throws Exception {
        Partition partition = newPartition("negative-length");
        partition.append(message("a"));
        partition.append(message("b"));

        // A length field of -4, which cancels its own four bytes.
        appendRawBytes(tempDir.resolve("negative-length/00000000000000000000.log"),
                ByteBuffer.allocate(4).putInt(-4).array());

        try {
            assertEquals(List.of("a", "b"), readAll(partition),
                    "the corrupt record must end the read, not be allocated for");
        } finally {
            partition.close();
        }
    }

    /**
     * Deriving the log end offset means walking the last segment counting records. A
     * negative length moves the position backwards, and -4 cancels the four bytes just
     * read, so the walk would never terminate and the broker would never start.
     */
    @Test
    void aCorruptLengthInTheLogDoesNotHangTheRestart() throws Exception {
        Path dir = tempDir.resolve("corrupt-count");
        Files.createDirectories(dir);

        byte[] payload = message("a");
        ByteBuffer log = ByteBuffer.allocate(4 + payload.length + 4);
        log.putInt(payload.length).put(payload).putInt(-4);
        Files.write(dir.resolve("00000000000000000000.log"), log.array());
        Files.write(dir.resolve("00000000000000000000.index"), new byte[16]);

        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            Partition partition = new Partition(0, 1, List.of(2), dir.toString());
            try {
                assertEquals(1, partition.getLogEndOffset(),
                        "counting must stop at the corrupt record instead of spinning on it");
            } finally {
                partition.close();
            }
        });
    }

    /**
     * A truncation that fails partway used to leave the partition with no open log at
     * all: the handles were released before the files were cut, so every later append
     * found nothing to write to. Whatever went wrong, the partition has to come back
     * to a state where it can still take writes.
     */
    @Test
    void aTruncationThatFailsPartwayLeavesThePartitionUsable() throws Exception {
        Partition partition = newPartition("failed-truncate");
        partition.append(message("a"));
        partition.append(message("b"));
        partition.append(message("c"));

        // Cutting the log succeeds, cutting the index does not - the failure lands after
        // the handles have already been released.
        Path index = tempDir.resolve("failed-truncate/00000000000000000000.index");
        assertTrue(index.toFile().setWritable(false), "could not make the index read-only");

        try {
            assertEquals(-2, partition.appendAt(1, message("B")),
                    "a truncation that could not complete must be reported, not assumed");
        } finally {
            index.toFile().setWritable(true);
        }

        // Losing the diverged suffix is the whole point of a truncation, so "a" survives
        // and "b"/"c" do not. What must not happen is the partition staying unusable.
        assertEquals(1, partition.append(message("d")),
                "the partition must recover to a state that can still be appended to");
        assertEquals(List.of("a", "d"), readAll(partition));
        assertEquals(2, partition.getLogEndOffset());
        partition.close();
    }

    /**
     * A reader can start earlier than the offset it was asked for: the index falls back
     * to the last entry it has and the read walks forward from there. What comes back has
     * to say where it really starts, because everything upstream - the broker's reply,
     * the client's cursor - is positioned by those offsets. Counting forwards from the
     * request instead would hand consumers offsets whose messages they never received.
     */
    @Test
    void readsReportTheOffsetEachRecordActuallyOccupies() throws Exception {
        Partition partition = newPartition("lazy-index");
        partition.append(message("a"));
        partition.append(message("b"));
        partition.append(message("c"));

        // Cut the index down to its first entry, so a read at offset 2 has to fall back
        // to offset 0 and walk forward.
        Path index = tempDir.resolve("lazy-index/00000000000000000000.index");
        try (FileChannel channel = FileChannel.open(index, StandardOpenOption.WRITE)) {
            channel.truncate(16);
        }

        List<Protocol.Record> records = partition.readMessages(2, 16 * 1024 * 1024);
        partition.close();

        assertEquals(3, records.size(), "the fallback reads from offset 0, so all three come back");
        assertEquals(0, records.get(0).getOffset(), "the reader must report where it really started");
        assertEquals(1, records.get(1).getOffset());
        assertEquals(2, records.get(2).getOffset());
        assertEquals("a", text(records.get(0).getPayload()));
        assertEquals("c", text(records.get(2).getPayload()));
    }

    private static void appendRawBytes(Path file, byte[] bytes) throws Exception {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.position(channel.size());
            Partition.writeFully(channel, ByteBuffer.wrap(bytes));
        }
    }

    private static String prefix(int index) {
        return "m" + index + "-";
    }

    private static byte[] payload(int index, int size) {
        byte[] prefix = prefix(index).getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[size];
        System.arraycopy(prefix, 0, data, 0, prefix.length);
        java.util.Arrays.fill(data, prefix.length, data.length, (byte) ('a' + index));
        return data;
    }
}
