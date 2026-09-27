package io.github.rcgeorge23.redispatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Publishes typed payloads to a Redis Stream and manages consumer-group delivery, stale recovery,
 * acknowledgement, and dead-letter retention.
 *
 * @param <T> payload type
 */
public final class RedisStreamQueue<T> implements MessageQueue<T> {

    static final String PAYLOAD_FIELD = "payload";
    static final String FAILURE_CODE_FIELD = "failureCode";
    static final String ORIGINAL_MESSAGE_ID_FIELD = "originalMessageId";
    private static final String DESERIALISATION_FAILURE = "deserialisation_failed";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final JavaType payloadType;
    private final RedisStreamQueueOptions options;
    private final Object groupLock = new Object();
    private volatile boolean consumerGroupCreated;

    /**
     * Creates a queue adapter using the supplied Redis connection, JSON mapper and payload type.
     *
     * @param redisTemplate connection template using string key/value serialization
     * @param objectMapper JSON serializer and deserializer
     * @param payloadType Jackson type used to decode queue payloads
     * @param options stream, consumer-group and retention settings
     * @throws NullPointerException if any dependency is null
     */
    public RedisStreamQueue(StringRedisTemplate redisTemplate, ObjectMapper objectMapper,
                            JavaType payloadType, RedisStreamQueueOptions options) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
    }

    /** Adds one payload and returns its Redis Stream entry ID. */
    public String publish(T payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        ensureConsumerGroup();
        String serialized;
        try {
            serialized = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to serialize queue payload", exception);
        }
        RecordId id = redisTemplate.opsForStream().add(
                MapRecord.create(options.streamKey(), Map.of(PAYLOAD_FIELD, serialized)));
        if (id == null) {
            throw new IllegalStateException("Redis did not return an ID for the published queue message");
        }
        return id.getValue();
    }

    // Spring Data Redis models stream offsets as generic varargs even when this call passes one typed offset.
    @SuppressWarnings("unchecked")
    @Override
    public List<QueueMessage<T>> pollBatch(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        ensureConsumerGroup();
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(options.consumerGroup(), options.consumerName()),
                StreamReadOptions.empty().count(limit).block(options.readBlockTimeout()),
                StreamOffset.create(options.streamKey(), ReadOffset.lastConsumed()));
        if (records == null || records.isEmpty()) {
            return List.of();
        }
        List<QueueMessage<T>> messages = new ArrayList<>(records.size());
        for (MapRecord<String, Object, Object> record : records) {
            decodeOrDeadLetter(record).ifPresent(messages::add);
        }
        return List.copyOf(messages);
    }

    @Override
    public void acknowledge(QueueMessage<T> message) {
        Objects.requireNonNull(message, "message must not be null");
        acknowledgeRecord(message.messageId());
    }

    @Override
    public void release(QueueMessage<T> message) {
        Objects.requireNonNull(message, "message must not be null");
        // Leave it pending; reclaimStale makes it available after the idle threshold.
    }

    @Override
    public void deadLetter(QueueMessage<T> message, String failureCode) {
        Objects.requireNonNull(message, "message must not be null");
        if (failureCode == null || failureCode.isBlank()) {
            throw new IllegalArgumentException("failureCode must not be blank");
        }
        String payload;
        try {
            payload = objectMapper.writerFor(payloadType).writeValueAsString(message.payload());
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to serialize queue payload for dead lettering", exception);
        }
        writeDeadLetter(payload, message.messageId(), failureCode);
        acknowledgeRecord(message.messageId());
    }

    @Override
    public List<QueueMessage<T>> reclaimStale(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        ensureConsumerGroup();
        PendingMessages pendingMessages = redisTemplate.opsForStream()
                .pending(options.streamKey(), options.consumerGroup(), Range.unbounded(), limit,
                        options.reclaimMinIdle());
        if (pendingMessages == null) {
            return List.of();
        }
        List<QueueMessage<T>> reclaimed = new ArrayList<>();
        for (PendingMessage pending : pendingMessages) {
            if (pending.getElapsedTimeSinceLastDelivery().compareTo(options.reclaimMinIdle()) < 0) {
                continue;
            }
            List<MapRecord<String, Object, Object>> claimed = redisTemplate.opsForStream().claim(
                    options.streamKey(), options.consumerGroup(), options.consumerName(),
                    options.reclaimMinIdle(), RecordId.of(pending.getIdAsString()));
            if (claimed == null || claimed.isEmpty()) {
                continue;
            }
            decodeOrDeadLetter(claimed.getFirst()).ifPresent(reclaimed::add);
        }
        return List.copyOf(reclaimed);
    }

    @Override
    public long backlogSize() {
        return Optional.ofNullable(redisTemplate.opsForStream().size(options.streamKey())).orElse(0L);
    }

    @Override
    public long deadLetterSize() {
        return Optional.ofNullable(redisTemplate.opsForStream().size(options.deadLetterStreamKey())).orElse(0L);
    }

    @Override
    public long oldestPendingIdleMillis() {
        ensureConsumerGroup();
        PendingMessages pendingMessages = redisTemplate.opsForStream()
                .pending(options.streamKey(), options.consumerGroup(), Range.unbounded(), 1);
        if (pendingMessages == null) {
            return -1L;
        }
        for (PendingMessage pending : pendingMessages) {
            return Math.max(0L, pending.getElapsedTimeSinceLastDelivery().toMillis());
        }
        return -1L;
    }

    /** Deletes both streams. Call this only when consumers are quiescent. */
    @Override
    public void purge() {
        synchronized (groupLock) {
            redisTemplate.delete(List.of(options.streamKey(), options.deadLetterStreamKey()));
            consumerGroupCreated = false;
            createConsumerGroup();
            consumerGroupCreated = true;
        }
    }

    @Override
    public void trimDeadLetter(long maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize must not be negative");
        }
        redisTemplate.opsForStream().trim(options.deadLetterStreamKey(), maxSize);
    }

    private Optional<QueueMessage<T>> decodeOrDeadLetter(MapRecord<String, Object, Object> record) {
        Objects.requireNonNull(record, "record must not be null");
        RecordId recordId = Objects.requireNonNull(record.getId(), "record ID must not be null");
        String messageId = Objects.requireNonNull(recordId.getValue(), "record ID value must not be null");
        String payload = rawPayload(record);
        try {
            if (payload == null) {
                throw new IllegalArgumentException("Queue record has no payload field");
            }
            T decoded = objectMapper.readValue(payload, payloadType);
            return Optional.of(new QueueMessage<>(messageId, decoded));
        } catch (Exception exception) {
            writeDeadLetter(payload, messageId, DESERIALISATION_FAILURE);
            acknowledgeRecord(messageId);
            return Optional.empty();
        }
    }

    private String rawPayload(MapRecord<String, Object, Object> record) {
        Map<Object, Object> fields = record.getValue();
        Object payload = fields == null ? null : fields.get(PAYLOAD_FIELD);
        return payload == null ? null : payload.toString();
    }

    private void writeDeadLetter(String payload, String messageId, String failureCode) {
        RecordId deadLetterId = redisTemplate.opsForStream().add(MapRecord.create(options.deadLetterStreamKey(), Map.of(
                PAYLOAD_FIELD, payload == null ? "" : payload,
                FAILURE_CODE_FIELD, failureCode,
                ORIGINAL_MESSAGE_ID_FIELD, messageId)));
        if (deadLetterId == null) {
            throw new IllegalStateException("Redis did not return an ID for the dead-letter message");
        }
        trimDeadLetter(options.deadLetterMaxSize());
    }

    private void acknowledgeRecord(String messageId) {
        RecordId id = RecordId.of(messageId);
        redisTemplate.opsForStream().acknowledge(options.streamKey(), options.consumerGroup(), id);
        redisTemplate.opsForStream().delete(options.streamKey(), id);
    }

    private void ensureConsumerGroup() {
        if (consumerGroupCreated) {
            return;
        }
        synchronized (groupLock) {
            if (consumerGroupCreated) {
                return;
            }
            createConsumerGroup();
            consumerGroupCreated = true;
        }
    }

    private void createConsumerGroup() {
        try {
            redisTemplate.opsForStream().createGroup(
                    options.streamKey(), ReadOffset.from("0"), options.consumerGroup());
        } catch (RuntimeException exception) {
            if (!isExistingGroup(exception)) {
                throw exception;
            }
        }
    }

    private static boolean isExistingGroup(Throwable exception) {
        for (Throwable current = exception; current != null; current = current.getCause()) {
            String message = current.getMessage();
            if (message != null && message.contains("BUSYGROUP")) {
                return true;
            }
        }
        return false;
    }
}
