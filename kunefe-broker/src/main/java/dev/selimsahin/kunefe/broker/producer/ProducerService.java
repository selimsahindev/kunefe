package dev.selimsahin.kunefe.broker.producer;

import dev.selimsahin.kunefe.broker.log.LogManager;
import dev.selimsahin.kunefe.broker.topic.TopicNotFoundException;
import dev.selimsahin.kunefe.broker.topic.TopicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

/**
 * Handles message publishing to topics.
 * <p>
 * Intentionally simple — no batching, no partitioning, no retry logic.
 * This is a conscious trade-off: Kunefe optimizes for low latency and
 * simplicity over Kafka's high-throughput batching model.
 * <p>
 * Each publish is a synchronous, sequential write to the topic log.
 * At-least-once delivery is guaranteed by the append-only log structure.
 */
@Service
public class ProducerService {

    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private final LogManager logManager;
    private final TopicService topicService;

    public ProducerService(LogManager logManager, TopicService topicService) {
        this.logManager = logManager;
        this.topicService = topicService;
    }

    /**
     * Publishes a message to the given topic.
     * <p>
     * Validates topic existence before writing. Increments the topic's
     * message count metadata after a successful write.
     *
     * @param topic   the target topic name
     * @param payload the raw message bytes
     * @param headers optional key-value metadata attached to the message
     * @return the offset assigned to the published message
     */
    public long publish(String topic, byte[] payload, Map<String, String> headers) throws IOException {
        if (!topicService.topicExists(topic)) {
            throw new TopicNotFoundException(topic);
        }

        long offset = logManager.append(topic, payload, headers);
        topicService.incrementMessageCount(topic);

        log.debug("Message published to topic '{}' at offset {}", topic, offset);
        return offset;
    }
}