package dev.selimsahin.kunefe.broker.log;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central manager for all topic logs.
 * <p>
 * Maintains a registry of TopicLog instances, one per topic.
 * Handles lifecycle - creation, recovery on startup, and graceful shutdown.
 * <p>
 * Uses ConcurrentHashMap for thread-safe topic registry without
 * locking the entire map on every read.
 */
@Component
public class LogManager {

    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    @Value("${kunefe.data.dir:./kunefe-data}")
    private String dataDir;

    private final Map<String, TopicLog> topicLogs = new ConcurrentHashMap<>();
    private Path dataDirPath;

    /**
     * On startup, ensures the data directory exists and recovers
     * any existing topic logs from disk.
     */
    @PostConstruct
    public void init() throws IOException {
        dataDirPath = Paths.get(dataDir);
        Files.createDirectories(dataDirPath);
        log.info("LogManager initialized - data dir: {}", dataDirPath.toAbsolutePath());
        recoverExistingLogs();
    }

    /**
     * Appends a message to the given topic's log.
     * Creates the topic log if it does not exist yet.
     *
     * @return the offset assigned to the message
     */
    public long append(String topic, byte[] payload, Map<String, String> headers) throws IOException {
        TopicLog topicLog = getOrCreateLog(topic);
        return topicLog.append(payload, headers);
    }

    /**
     * Reads all messages from the given topic starting at fromOffset.
     */
    public List<LogEntry> readFrom(String topic, long fromOffset) throws IOException {
        TopicLog topicLog = getOrCreateLog(topic);
        return topicLog.readFrom(fromOffset);
    }

    /**
     * Returns the next offset for the given topic.
     * Used by consumers to know where the log ends.
     */
    public long getNextOffset(String topic) throws IOException {
        TopicLog topicLog = getOrCreateLog(topic);
        return topicLog.getNextOffset();
    }

    /**
     * Checks whether a topic log exists.
     */
    public boolean topicExists(String topic) {
        return topicLogs.containsKey(topic) ||
                dataDirPath.resolve(topic + ".log").toFile().exists();
    }

    /**
     * Creates a new topic log explicitly.
     * Called by BrokerService when a topic is created via gRPC.
     */
    public void createTopic(String topic) throws IOException {
        if (topicExists(topic)) {
            log.warn("Topic '{}' already exists, skipping creation", topic);
            return;
        }
        getOrCreateLog(topic);
        log.info("Topic '{}' created", topic);
    }

    /**
     * Returns all currently known topic names.
     */
    public List<String> listTopics() {
        return List.copyOf(topicLogs.keySet());
    }

    /**
     * Gets the TopicLog for the given topic, creating it if necessary.
     * computeIfAbsent is atomic - only one TopicLog is ever created per topic.
     */
    private TopicLog getOrCreateLog(String topic) throws IOException {
        TopicLog topicLog = topicLogs.get(topic);
        if (topicLog != null) {
            return topicLog;
        }

        try {
            return topicLogs.computeIfAbsent(topic, t -> {
                try {
                    return new TopicLog(t, dataDirPath);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create TopicLog for topic: " + t, e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw e;
        }
    }

    /**
     * On startup, scans the data directory for existing .log files
     * and recovers them into memory. This restores state after a restart.
     */
    private void recoverExistingLogs() throws IOException {
        File[] logFiles = dataDirPath.toFile().listFiles(
                (dir, name) -> name.endsWith(".log")
        );

        if (logFiles == null || logFiles.length == 0) {
            log.info("No existing topic logs found — starting fresh");
            return;
        }

        for (File logFile : logFiles) {
            String topic = logFile.getName().replace(".log", "");
            TopicLog topicLog = new TopicLog(topic, dataDirPath);
            topicLogs.put(topic, topicLog);
            log.info("Recovered topic log: '{}'", topic);
        }
    }

    /**
     * Gracefully closes all topic logs on shutdown.
     * Ensures MappedByteBuffer is flushed to disk before exit.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down LogManager — flushing {} topic logs", topicLogs.size());
        topicLogs.values().forEach(topicLog -> {
            try {
                topicLog.close();
            } catch (IOException e) {
                log.error("Failed to close topic log: {}", topicLog.getTopic(), e);
            }
        });
    }
}