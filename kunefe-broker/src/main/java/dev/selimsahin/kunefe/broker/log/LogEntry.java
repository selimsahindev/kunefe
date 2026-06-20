package dev.selimsahin.kunefe.broker.log;

import java.util.Map;

/**
 * Represents a single log entry persisted on disk.
 * <p>
 * Binary format:
 * [offset: 8 bytes][timestamp: 8 bytes][headersLen: 4 bytes][payloadLen: 4 bytes][payload][headers]
 * <p>
 * Defined as a record to guarantee immutability.
 * Since the log is append-only, entries are written once and never updated.
 */
public record LogEntry(
        long offset,
        long timestamp,
        byte[] payload,
        Map<String, String> headers
) {

    public static final int FIXED_HEADER_SIZE = 24; // 8 + 8 + 4 + 4

    /**
     * Returns the total size of this entry in bytes when stored on disk.
     * Used to determine the buffer size required for serialization.
     */
    public int totalSize() {
        return FIXED_HEADER_SIZE + payload.length + headersSize();
    }

    private int headersSize() {
        if (headers == null || headers.isEmpty()) {
            return 0;
        }

        int size = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            // Each header: [keyLen: 4 byte][key bytes][valueLen: 4 byte][value bytes]
            size += 4 + entry.getKey().getBytes().length;
            size += 4 + entry.getValue().getBytes().length;
        }
        return size;
    }
}