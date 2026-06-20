package dev.selimsahin.kunefe.broker.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Append-only log for a single topic backed by a memory-mapped file.
 * <p>
 * Each message is written sequentially to disk using MappedByteBuffer,
 * which allows OS-level memory mapping for near-RAM write performance
 * while guaranteeing durability across restarts.
 * <p>
 * Thread safety is achieved via a ReadWriteLock:
 * - Multiple concurrent readers are allowed
 * - Writes are exclusive
 */
public class TopicLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopicLog.class);
    private static final long DEFAULT_MAX_FILE_SIZE = 1024L * 1024L * 512L; // 512MB

    private final String topic;
    private final MappedByteBuffer buffer;
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;

    public TopicLog(String topic, Path dataDir) throws IOException {
        this.topic = topic;
        this.lock = new ReentrantReadWriteLock();
        this.nextOffset = new AtomicLong(0);

        Path logFile = dataDir.resolve(topic + ".log");
        this.file = new RandomAccessFile(logFile.toFile(), "rw");
        this.channel = file.getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, DEFAULT_MAX_FILE_SIZE);

        recoverOffset();
    }

    /**
     * Appends a message to the log and returns its assigned offset.
     * <p>
     * Write format per entry:
     * [offset: 8B][timestamp: 8B][headersLen: 4B][payloadLen: 4B][payload][headers]
     */
    public long append(byte[] payload, Map<String, String> headers) {
        lock.writeLock().lock();
        try {
            long offset = nextOffset.getAndIncrement();
            long timestamp = System.currentTimeMillis();

            byte[] headersBytes = serializeHeaders(headers);

            buffer.putLong(offset);
            buffer.putLong(timestamp);
            buffer.putInt(headersBytes.length);
            buffer.putInt(payload.length);
            buffer.put(payload);
            buffer.put(headersBytes);
            buffer.force();

            log.debug("Appended message to topic '{}' at offset {}", topic, offset);
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Reads all messages starting from the given offset.
     */
    public List<LogEntry> readFrom(long fromOffset) {
        lock.readLock().lock();
        try {
            List<LogEntry> entries = new ArrayList<>();
            MappedByteBuffer readBuffer = buffer.duplicate();
            readBuffer.position(0);

            while (readBuffer.remaining() >= LogEntry.FIXED_HEADER_SIZE) {
                long offset = readBuffer.getLong();
                long timestamp = readBuffer.getLong();
                int headersLen = readBuffer.getInt();
                int payloadLen = readBuffer.getInt();

                if (payloadLen == 0 && offset == 0 && timestamp == 0) {
                    break;
                }

                byte[] payload = new byte[payloadLen];
                readBuffer.get(payload);

                byte[] headersBytes = new byte[headersLen];
                readBuffer.get(headersBytes);
                Map<String, String> headers = deserializeHeaders(headersBytes);

                if (offset >= fromOffset) {
                    entries.add(new LogEntry(offset, timestamp, payload, headers));
                }
            }

            return entries;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getNextOffset() {
        return nextOffset.get();
    }

    public String getTopic() {
        return topic;
    }

    /**
     * Scans the existing log file on startup to recover the next offset.
     * This ensures offset continuity across broker restarts.
     */
    private void recoverOffset() {
        MappedByteBuffer readBuffer = buffer.duplicate();
        readBuffer.position(0);
        long lastOffset = -1;

        while (readBuffer.remaining() >= LogEntry.FIXED_HEADER_SIZE) {
            long offset = readBuffer.getLong();
            long timestamp = readBuffer.getLong();
            int headersLen = readBuffer.getInt();
            int payloadLen = readBuffer.getInt();

            if (payloadLen == 0 && offset == 0 && timestamp == 0) {
                break;
            }

            lastOffset = offset;
            readBuffer.position(readBuffer.position() + payloadLen + headersLen);
        }

        nextOffset.set(lastOffset + 1);
        buffer.position(readBuffer.position());
        log.info("Recovered topic '{}' — next offset: {}", topic, nextOffset.get());
    }

    /**
     * Serializes headers map into bytes.
     * Format per entry: [keyLen: 4B][key bytes][valueLen: 4B][value bytes]
     */
    private byte[] serializeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return new byte[0];
        }

        int totalSize = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            totalSize += 4 + entry.getKey().getBytes(StandardCharsets.UTF_8).length;
            totalSize += 4 + entry.getValue().getBytes(StandardCharsets.UTF_8).length;
        }

        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(totalSize);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
            buf.putInt(valueBytes.length);
            buf.put(valueBytes);
        }

        return buf.array();
    }

    /**
     * Deserializes bytes back into a headers map.
     */
    private Map<String, String> deserializeHeaders(byte[] bytes) {
        Map<String, String> headers = new HashMap<>();
        if (bytes.length == 0) {
            return headers;
        }

        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
        while (buf.remaining() > 0) {
            int keyLen = buf.getInt();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);

            int valueLen = buf.getInt();
            byte[] valueBytes = new byte[valueLen];
            buf.get(valueBytes);

            headers.put(
                    new String(keyBytes, StandardCharsets.UTF_8),
                    new String(valueBytes, StandardCharsets.UTF_8)
            );
        }

        return headers;
    }

    @Override
    public void close() throws IOException {
        buffer.force();
        channel.close();
        file.close();
        log.info("TopicLog '{}' closed", topic);
    }
}