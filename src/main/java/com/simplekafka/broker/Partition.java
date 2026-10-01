package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a partition in Build Your Own Kafka, containing segments of message logs
 */
public class Partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024; // 1MB segment size
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";
    
    private final int id;
    private int leader;
    private List<Integer> followers;
    private final String baseDir;
    private final boolean localReplica;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private final List<SegmentInfo> segments;
    
    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this(id, leader, followers, baseDir, true);
    }
    
    /**
     * @param localReplica whether this broker is the leader or one of the followers of
     *        the partition and therefore actually stores its log. A broker that only
     *        knows about a partition (so it can answer metadata requests and forward
     *        writes to the leader) keeps the assignment without creating any files:
     *        an empty log of its own would let the broker start writing at offset 0
     *        if it were ever elected leader for real data.
     */
    public Partition(int id, int leader, List<Integer> followers, String baseDir, boolean localReplica) {
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.localReplica = localReplica;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        
        if (localReplica) {
            initialize();
        } else {
            LOGGER.info("Partition " + id + " is not replicated on this broker, keeping its assignment only");
        }
    }
    
    /**
     * Whether this broker stores the log of this partition
     */
    public boolean isLocalReplica() {
        return localReplica;
    }
    
    /**
     * Initialize partition and load existing segments
     */
    private void initialize() {
        try {
            // Create directory if it doesn't exist
            File dir = new File(baseDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            
            // Load existing segments
            File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String baseName = file.getName().substring(0, file.getName().length() - LOG_SUFFIX.length());
                    long baseOffset = Long.parseLong(baseName);
                    
                    File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
                    if (indexFile.exists()) {
                        SegmentInfo segment = new SegmentInfo(baseOffset, file.getAbsolutePath(), indexFile.getAbsolutePath());
                        segments.add(segment);
                    }
                }
                
                // Sort segments by base offset
                segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));
                
                // Determine next offset from the last segment
                if (!segments.isEmpty()) {
                    SegmentInfo lastSegment = segments.get(segments.size() - 1);
                    nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment));
                }
            }
            
            // Create a new segment if none exists
            if (segments.isEmpty()) {
                createNewSegment(0);
            } else {
                // Open the last segment as active
                SegmentInfo lastSegment = segments.get(segments.size() - 1);
                openSegmentForAppend(lastSegment);
            }
            
            LOGGER.info("Initialized partition " + id + " with " + segments.size() + 
                       " segments, next offset: " + nextOffset.get());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
        }
    }
    
    /**
     * Count the number of messages in a segment
     */
    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0;
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
             FileChannel logChannel = logFile.getChannel()) {
            
            ByteBuffer buffer = ByteBuffer.allocate(4); // Size field is 4 bytes
            long size = logChannel.size();

            while (logChannel.position() < size) {
                buffer.clear();
                int bytesRead = logChannel.read(buffer);
                if (bytesRead < 4) break;

                buffer.flip();
                int messageSize = buffer.getInt();

                // The size field comes off the disk, so it is only as trustworthy as
                // the file. A negative one would move the position backwards - and a
                // length of -4 exactly cancels the four bytes just read, spinning here
                // forever; one that runs past the end means the log is torn.
                if (messageSize < 0 || logChannel.position() + messageSize > size) {
                    LOGGER.warning("Corrupt message length " + messageSize +
                            " while counting partition " + id + "; stopping at " + count);
                    break;
                }

                // Skip the message
                logChannel.position(logChannel.position() + messageSize);
                count++;
            }
        }
        
        return count;
    }
    
    /**
     * Create a new segment for this partition
     */
    private void createNewSegment(long baseOffset) throws IOException {
        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX;
        
        // Create log file
        File logFile = new File(logPath);
        logFile.createNewFile();
        
        // Create index file
        File indexFile = new File(indexPath);
        indexFile.createNewFile();
        
        // Add to segments list
        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath);
        segments.add(segment);
        
        // Open for append
        openSegmentForAppend(segment);
        
        LOGGER.info("Created new segment for partition " + id + ", base offset: " + baseOffset);
    }
    
    /**
     * Open a segment for append operations
     */
    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // Close the currently active segment if any
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }
        
        if (activeLogFile != null) {
            activeLogFile.close();
        }
        
        // Open the segment
        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();
        
        // Move to the end of the file for appending
        activeLogChannel.position(activeLogChannel.size());
    }
    
    /**
     * Append a message to the log.
     * @return the offset where the message was appended
     */
    public long append(byte[] message) {
        if (!localReplica) {
            // This broker does not store the partition, so it must not start a log
            // of its own at offset 0 while the real replicas already hold data.
            LOGGER.warning("Partition " + id + " has no local log on this broker");
            return -1;
        }

        lock.writeLock().lock();
        try {
            return doAppend(message);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Append a message at the offset picked by the partition leader.
     *
     * <p>A replica never chooses its own offsets: if it did, the same offset could
     * hold different data on different brokers once a message was lost or arrives
     * out of order, and consumers reading that offset would see different data
     * depending on which replica they hit.
     *
     * @param offset the offset the leader assigned to {@code message}
     * @param message the message body
     * @return {@code offset} when the replica's log now holds {@code message} at
     *         {@code offset}; {@code -1} when {@code offset} is ahead of this
     *         replica's log end offset (messages are missing); {@code -2} on failure
     */
    public long appendAt(long offset, byte[] message) {
        if (!localReplica || offset < 0) {
            return -2;
        }

        lock.writeLock().lock();
        try {
            long logEndOffset = nextOffset.get();

            if (offset > logEndOffset) {
                // The leader is ahead of us: messages [logEndOffset, offset) never
                // arrived. Refuse instead of silently writing at the wrong place.
                return -1;
            }

            if (offset < logEndOffset) {
                byte[] existing;
                try {
                    existing = readMessageAt(offset);
                } catch (IOException e) {
                    // We could not read our own log, which is not evidence that it
                    // disagrees with the leader's. Refusing is the safe answer; the
                    // alternative below destroys a suffix that may well be correct.
                    LOGGER.log(Level.WARNING, "Cannot read offset " + offset + " of partition " + id, e);
                    return -2;
                }

                if (existing != null && Arrays.equals(existing, message)) {
                    // A retry of a request we already applied: nothing to do.
                    return offset;
                }

                // Our log no longer matches the leader's from this offset onwards.
                // Drop the diverged suffix and rewrite the leader's version.
                LOGGER.warning("Log of partition " + id + " diverged from the leader at offset " +
                        offset + "; truncating");
                if (!truncateFrom(offset)) {
                    return -2;
                }
            }

            long written = doAppend(message);
            return written == offset ? offset : -2;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Append at whatever the current end of the log is. Callers must hold the write lock.
     * @return the offset where the message was appended, or -1 on failure
     */
    private long doAppend(byte[] message) {
        if (activeLogChannel == null || !activeLogChannel.isOpen()) {
            // initialize() gave up, or a truncation failed partway. Reporting it beats
            // throwing an NPE at whoever asked, and beats the far worse alternative of
            // writing at an offset nobody chose.
            LOGGER.warning("Partition " + id + " has no open log to append to");
            return -1;
        }

        try {
            long currentOffset = nextOffset.get();

            // Check if we need to roll over to a new segment
            if (activeLogChannel.position() >= DEFAULT_SEGMENT_SIZE) {
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);
            }

            // Write message size and data
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();

            // Write to file, remembering where so a short write can be undone
            long position = activeLogChannel.position();
            try {
                writeFully(activeLogChannel, buffer);
                activeLogChannel.force(true);
            } catch (IOException e) {
                // Leave the file exactly as long as it was. A half-written record would
                // be counted as a message by countMessagesInSegment on the next start,
                // and every offset after it would describe the wrong bytes.
                rollBackTo(position);
                throw e;
            }

            // Update index
            updateIndex(currentOffset, position);

            // Update offset
            nextOffset.incrementAndGet();

            return currentOffset;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to append message to partition " + id, e);
            return -1;
        }
    }

    /**
     * Write every byte of the buffer to the file, refusing to accept a short write.
     *
     * <p>{@code FileChannel.write} may transfer fewer bytes than asked when the disk
     * fills or the write is interrupted, and this log has no framing of its own to
     * recover from: a reader trusts each record's size field, so a record that is not
     * all there makes everything after it unreadable.
     */
    static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.write(buffer) <= 0) {
                throw new IOException("Short write: " + buffer.remaining() + " bytes still to go");
            }
        }
    }

    /**
     * Cut the log back to {@code position} after a failed append, so the next append
     * starts where this one did and the offset-to-position mapping stays true.
     */
    private void rollBackTo(long position) {
        try {
            activeLogChannel.truncate(position);
            activeLogChannel.position(position);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to roll partition " + id + " back to " + position, e);
        }
    }

    /**
     * Read the message stored at a single offset, or null when it cannot be read.
     * Used by {@link #appendAt} to decide whether an existing offset already holds
     * the leader's copy of the message.
     */
    private byte[] readMessageAt(long offset) throws IOException {
        SegmentInfo segment = findSegmentForOffset(offset);
        if (segment == null) {
            return null;
        }

        long position = scanPositionForOffset(segment, offset - segment.getBaseOffset());
        if (position < 0) {
            return null;
        }

        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
             FileChannel logChannel = logFile.getChannel()) {

            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            logChannel.position(position);
            if (logChannel.read(sizeBuffer) < 4) {
                return null;
            }
            sizeBuffer.flip();

            int messageSize = sizeBuffer.getInt();
            if (messageSize < 0 || position + 4 + messageSize > logChannel.size()) {
                return null;
            }

            ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
            if (logChannel.read(messageBuffer) < messageSize) {
                return null;
            }
            messageBuffer.flip();

            byte[] message = new byte[messageSize];
            messageBuffer.get(message);
            return message;
        }
    }

    /**
     * Byte position of the message with the given offset relative to the start of
     * the segment, found by walking the file. Walking is cheap for a segment and,
     * unlike the index lookup, it cannot be off by one message.
     */
    private long scanPositionForOffset(SegmentInfo segment, long relativeOffset) throws IOException {
        if (relativeOffset < 0) {
            return -1;
        }

        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
             FileChannel logChannel = logFile.getChannel()) {

            long position = 0;
            long current = 0;
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

            while (position < logChannel.size()) {
                if (current == relativeOffset) {
                    return position;
                }

                sizeBuffer.clear();
                logChannel.position(position);
                if (logChannel.read(sizeBuffer) < 4) {
                    return -1;
                }
                sizeBuffer.flip();

                int messageSize = sizeBuffer.getInt();
                if (messageSize < 0 || position + 4 + messageSize > logChannel.size()) {
                    return -1;
                }

                position += 4 + messageSize;
                current++;
            }

            return current == relativeOffset ? position : -1;
        }
    }

    /**
     * Drop every message from {@code offset} onwards so the log ends exactly where
     * the leader's log ends, ready for the leader's version to be rewritten.
     * Callers must hold the write lock.
     * @return true when the log now ends at {@code offset}
     */
    private boolean truncateFrom(long offset) {
        try {
            long logEndOffset = nextOffset.get();
            if (offset >= logEndOffset) {
                return true;
            }

            SegmentInfo target = findSegmentForOffset(offset);
            if (target == null) {
                return false;
            }

            long position = scanPositionForOffset(target, offset - target.getBaseOffset());
            if (position < 0) {
                return false;
            }

            // Remove every segment that starts at or after the diverged offset, before
            // anything else is touched. A segment that cannot be removed has to abort
            // the whole truncation: dropping it from the list while its file survives
            // would leave `segments` describing a log that is not the one on disk, and
            // its ordering is exactly what findSegmentForOffset's binary search relies
            // on - an unsorted list makes valid offsets unreadable.
            int targetIndex = segments.indexOf(target);
            for (int i = segments.size() - 1; i > targetIndex; i--) {
                SegmentInfo doomed = segments.get(i);
                boolean logGone = new File(doomed.getLogPath()).delete();
                boolean indexGone = new File(doomed.getIndexPath()).delete();
                if (!logGone || !indexGone) {
                    LOGGER.warning("Could not remove segment " + doomed.getBaseOffset() +
                            " of partition " + id + "; abandoning the truncation");
                    recoverState();
                    return false;
                }
                segments.remove(i);
            }

            // Release the active segment before modifying its files on disk.
            if (activeLogChannel != null) {
                activeLogChannel.close();
                activeLogChannel = null;
            }
            if (activeLogFile != null) {
                activeLogFile.close();
                activeLogFile = null;
            }

            // Cut the surviving segment right where the divergence starts.
            try (RandomAccessFile logFile = new RandomAccessFile(target.getLogPath(), "rw")) {
                logFile.setLength(position);
            }

            long relativeOffset = offset - target.getBaseOffset();
            try (RandomAccessFile indexFile = new RandomAccessFile(target.getIndexPath(), "rw")) {
                indexFile.setLength(relativeOffset * 16);
            }

            nextOffset.set(offset);
            openSegmentForAppend(target);

            LOGGER.info("Truncated partition " + id + " back to offset " + offset);
            return true;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to truncate partition " + id + " at offset " + offset, e);
            // The files and the in-memory view may now disagree, and the active channel
            // is gone. Rebuild the view from what is actually on disk rather than
            // leaving the partition unable to append at all.
            recoverState();
            return false;
        }
    }

    /**
     * Rebuild the in-memory view from what is actually on disk: forget segments whose
     * files are gone, restore the ordering, and take the log end offset from the last
     * survivor.
     *
     * <p>Used after a truncation fails partway. Cutting a log back is a prefix
     * operation, so there is never a half-written record to reason about, and deriving
     * the offset from the data is what a restart would do anyway - which is why this
     * is cheaper and no less correct than journalling the truncation.
     */
    private void recoverState() {
        try {
            for (int i = segments.size() - 1; i >= 0; i--) {
                if (!new File(segments.get(i).getLogPath()).exists()) {
                    segments.remove(i);
                }
            }
            segments.sort(Comparator.comparingLong(SegmentInfo::getBaseOffset));

            if (segments.isEmpty()) {
                nextOffset.set(0);
                createNewSegment(0);
                return;
            }

            SegmentInfo last = segments.get(segments.size() - 1);
            long messages = countMessagesInSegment(last);
            nextOffset.set(last.getBaseOffset() + messages);

            // The index may still hold entries for records that are gone, and the reader
            // addresses it by entry position - one entry per offset, in order. Cut it
            // back to what the log actually holds or every later lookup is off by one.
            //
            // Best effort on purpose: reopening the log is what makes the partition
            // usable again, and it must not depend on this. A trail of extra entries
            // past the log end is unreachable anyway, because reads at or beyond the log
            // end offset are refused before the index is consulted.
            try (RandomAccessFile indexFile = new RandomAccessFile(last.getIndexPath(), "rw")) {
                indexFile.setLength(messages * 16);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Could not trim the index of partition " + id, e);
            }

            openSegmentForAppend(last);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to recover partition " + id + " after a failed truncation", e);
        }
    }
    
    /**
     * Update the index file with new offset
     */
    private void updateIndex(long offset, long position) {
        try {
            // Find the current segment
            if (segments.isEmpty()) return;
            
            SegmentInfo currentSegment = segments.get(segments.size() - 1);
            
            try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw");
                 FileChannel indexChannel = indexFile.getChannel()) {
                
                // Position at the end
                indexChannel.position(indexChannel.size());
                
                // Write offset and position
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(offset);
                buffer.putLong(position);
                buffer.flip();
                
                indexChannel.write(buffer);
                indexChannel.force(true);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to update index for partition " + id, e);
        }
    }
    
    /**
     * Read messages from the log starting at offset
     */
    public List<Protocol.Record> readMessages(long offset, int maxBytes) {
        List<Protocol.Record> records = new ArrayList<>();
        if (!localReplica) {
            return records;
        }

        lock.readLock().lock();
        int bytesRead = 0;
        
        try {
            // Find the segment containing the offset
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if (targetSegment == null) {
                return records;
            }
            
            // Find the file position for the offset using the index
            IndexEntry entry = findPositionForOffset(targetSegment, offset);
            if (entry == null) {
                return records;
            }

            // The index can hand back an earlier offset than the one asked for, so the
            // cursor starts wherever the entry actually points rather than at the request.
            long position = entry.position;
            
            // Open the log file for reading. The handle is swapped for the next segment
            // when the read runs past the end of this one, so it is managed explicitly.
            RandomAccessFile logFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
            try {
                FileChannel logChannel = logFile.getChannel();
                
                // Position at the correct spot
                logChannel.position(position);
                
                // Read messages until maxBytes is reached
                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                long currentOffset = entry.offset;
                
                while (bytesRead < maxBytes && logChannel.position() < logChannel.size()) {
                    // Read message size
                    sizeBuffer.clear();
                    int sizeRead = logChannel.read(sizeBuffer);
                    if (sizeRead < 4) break;
                    
                    sizeBuffer.flip();
                    int messageSize = sizeBuffer.getInt();

                    // The length comes off the disk, so it is only as trustworthy as the
                    // file. Its two siblings already refuse one that runs past the end of
                    // the segment; without the same check here it would be handed straight
                    // to the allocator - a negative length throws, a huge one can exhaust
                    // the heap.
                    if (messageSize < 0 || logChannel.position() + messageSize > logChannel.size()) {
                        LOGGER.warning("Corrupt message length " + messageSize + " at offset " +
                                currentOffset + " of partition " + id);
                        break;
                    }

                    // Check if adding this message would exceed maxBytes
                    if (bytesRead + messageSize > maxBytes) {
                        break;
                    }
                    
                    // Read message data
                    ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                    int messageRead = logChannel.read(messageBuffer);
                    
                    if (messageRead < messageSize) {
                        LOGGER.warning("Incomplete message read at offset " + currentOffset);
                        break;
                    }
                    
                    messageBuffer.flip();
                    
                    // Add message to result, with the offset it really occupies
                    byte[] message = new byte[messageSize];
                    messageBuffer.get(message);
                    records.add(new Protocol.Record(currentOffset, message));
                    
                    // Update bytes read
                    bytesRead += messageSize + 4; // message size + 4 bytes for size field
                    currentOffset++;
                    
                    // If we're at the end of this segment, move to the next one
                    if (logChannel.position() >= logChannel.size() && currentOffset < nextOffset.get()) {
                        int nextSegmentIndex = segments.indexOf(targetSegment) + 1;
                        if (nextSegmentIndex < segments.size()) {
                            targetSegment = segments.get(nextSegmentIndex);
                            
                            // Carry on at the start of the next segment's file
                            logFile.close();
                            logFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                            logChannel = logFile.getChannel();
                            position = 0;
                            logChannel.position(position);
                            // A segment is named after the offset its first message
                            // occupies, so the cursor belongs there rather than where the
                            // previous segment happened to run out.
                            currentOffset = targetSegment.getBaseOffset();
                        }
                    }
                }
            } finally {
                logFile.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to read messages from partition " + id, e);
        } finally {
            lock.readLock().unlock();
        }

        return records;
    }
    
    /**
     * Find the segment containing the given offset
     */
    private SegmentInfo findSegmentForOffset(long offset) {
        if (segments.isEmpty() || offset >= nextOffset.get()) {
            return null;
        }
        
        // Binary search to find the segment
        int low = 0;
        int high = segments.size() - 1;
        
        while (low <= high) {
            int mid = (low + high) / 2;
            SegmentInfo segment = segments.get(mid);
            
            if (mid < segments.size() - 1) {
                SegmentInfo nextSegment = segments.get(mid + 1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()) {
                    return segment;
                }
            } else {
                // Last segment
                if (offset >= segment.getBaseOffset()) {
                    return segment;
                }
            }
            
            if (offset < segment.getBaseOffset()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        
        return null;
    }
    
    /**
     * Find the file position for the given offset using the index
     */
    private IndexEntry findPositionForOffset(SegmentInfo segment, long offset) {
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath(), "r");
             FileChannel indexChannel = indexFile.getChannel()) {
            
            if (indexChannel.size() < 16) {
                // Not one whole entry: a torn index, or none at all. Start at the
                // beginning of the log, which is the offset the segment's name gives.
                // Reading a last entry would seek to size() - 16, which for an index of
                // 1..15 bytes is a negative file position.
                return new IndexEntry(segment.getBaseOffset(), 0);
            }
            
            // Relative offset within the segment
            long relativeOffset = offset - segment.getBaseOffset();
            
            // Each index entry is 16 bytes (8 for offset, 8 for position)
            long entryCount = indexChannel.size() / 16;
            
            // Past the end of the index the last entry is the closest known point, and
            // the reader walks forward from there. That is an EARLIER offset than the one
            // asked for, which is exactly why the entry's own offset goes back with its
            // position: the caller can then describe what it returns truthfully instead
            // of assuming it starts where it was asked to.
            long entryIndex = Math.min(Math.max(relativeOffset, 0), entryCount - 1);

            indexChannel.position(entryIndex * 16);
            ByteBuffer buffer = ByteBuffer.allocate(16);
            indexChannel.read(buffer);
            buffer.flip();

            return new IndexEntry(buffer.getLong(), buffer.getLong());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to find position for offset " + offset, e);
            return null;
        }
    }

    /**
     * What an index lookup found: a byte position, and the offset that position actually
     * holds. The two differ whenever the lookup fell back to the last entry it had.
     */
    private static final class IndexEntry {
        private final long offset;
        private final long position;

        private IndexEntry(long offset, long position) {
            this.offset = offset;
            this.position = position;
        }
    }
    
    /**
     * Get partition ID
     */
    public int getId() {
        return id;
    }
    
    /**
     * Get leader broker ID
     */
    public int getLeader() {
        return leader;
    }
    
    /**
     * Set leader broker ID
     */
    public void setLeader(int leader) {
        this.leader = leader;
    }
    
    /**
     * Get follower broker IDs
     */
    public List<Integer> getFollowers() {
        return new ArrayList<>(followers);
    }
    
    /**
     * Set follower broker IDs
     */
    public void setFollowers(List<Integer> followers) {
        this.followers = new ArrayList<>(followers);
    }
    
    /**
     * Get the current log end offset
     */
    public long getLogEndOffset() {
        return nextOffset.get();
    }
    
    /**
     * Close partition resources
     */
    public void close() {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null && activeLogChannel.isOpen()) {
                activeLogChannel.close();
            }
            
            if (activeLogFile != null) {
                activeLogFile.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to close partition resources", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Class to represent segment information
     */
    private static class SegmentInfo {
        private final long baseOffset;
        private final String logPath;
        private final String indexPath;
        
        public SegmentInfo(long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }
        
        public long getBaseOffset() {
            return baseOffset;
        }
        
        public String getLogPath() {
            return logPath;
        }
        
        public String getIndexPath() {
            return indexPath;
        }
    }
}