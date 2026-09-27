package io.github.rcgeorge23.redispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RedisStreamQueueIntegrationTest {

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redisTemplate;
    private static ObjectMapper objectMapper;

    @BeforeAll
    static void connectToRedis() {
        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        connectionFactory = new LettuceConnectionFactory(host, port);
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        objectMapper = new ObjectMapper();
    }

    @AfterAll
    static void closeRedisConnection() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Test
    void publishesPollsAndAcknowledgesARealStreamRecord() {
        RedisStreamQueueOptions options = options(Duration.ofMillis(100), 5);
        RedisStreamQueue<Payload> queue = payloadQueue(options);
        Payload payload = new Payload("order-17", "donation received");

        String publishedId = queue.publish(payload);

        assertThat(queue.backlogSize()).isEqualTo(1);
        List<QueueMessage<Payload>> batch = queue.pollBatch(1);
        assertThat(batch).singleElement().satisfies(message -> {
            assertThat(message.messageId()).isEqualTo(publishedId);
            assertThat(message.payload()).isEqualTo(payload);
        });
        assertThat(queue.oldestPendingIdleMillis()).isGreaterThanOrEqualTo(0);

        queue.acknowledge(batch.getFirst());

        assertThat(queue.backlogSize()).isZero();
        assertThat(queue.oldestPendingIdleMillis()).isEqualTo(-1);
    }

    @Test
    void reclaimedReleasedMessageBecomesPendingAgainOnlyAfterIdleThreshold() throws InterruptedException {
        RedisStreamQueue<Payload> queue = payloadQueue(options(Duration.ofMillis(150), 5));
        String publishedId = queue.publish(new Payload("order-18", "ticket allocation"));
        QueueMessage<Payload> firstDelivery = queue.pollBatch(1).getFirst();

        queue.release(firstDelivery);
        assertThat(queue.pollBatch(1)).isEmpty();

        Thread.sleep(250);
        List<QueueMessage<Payload>> reclaimed = queue.reclaimStale(1);

        assertThat(reclaimed).singleElement().satisfies(message -> {
            assertThat(message.messageId()).isEqualTo(publishedId);
            assertThat(message.payload()).isEqualTo(firstDelivery.payload());
        });
        queue.acknowledge(reclaimed.getFirst());
        assertThat(queue.backlogSize()).isZero();
    }

    @Test
    void deadLetterEntriesAreTrimmedToTheConfiguredBound() {
        RedisStreamQueueOptions options = options(Duration.ofMillis(100), 2);
        RedisStreamQueue<String> queue = stringQueue(options);
        List<String> sourceIds = List.of(
                deadLetter(queue, "first"),
                deadLetter(queue, "second"),
                deadLetter(queue, "third"));

        assertThat(queue.deadLetterSize()).isEqualTo(2);
        assertThat(queue.backlogSize()).isZero();
        List<MapRecord<String, Object, Object>> entries = redisTemplate.opsForStream()
                .range(options.deadLetterStreamKey(), Range.unbounded());
        assertThat(entries)
                .extracting(record -> record.getValue().get(RedisStreamQueue.ORIGINAL_MESSAGE_ID_FIELD).toString())
                .containsExactly(sourceIds.get(1), sourceIds.get(2));
    }

    @Test
    void malformedStreamPayloadIsDeadLetteredAndAcknowledged() {
        RedisStreamQueueOptions options = options(Duration.ofMillis(100), 5);
        RedisStreamQueue<String> queue = stringQueue(options);
        assertThat(queue.pollBatch(1)).isEmpty(); // Creates the consumer group before inserting raw data.
        String sourceId = redisTemplate.opsForStream().add(MapRecord.create(
                options.streamKey(), Map.of(RedisStreamQueue.PAYLOAD_FIELD, "{not-json"))).getValue();

        assertThat(queue.pollBatch(1)).isEmpty();

        assertThat(queue.backlogSize()).isZero();
        assertThat(queue.deadLetterSize()).isEqualTo(1);
        MapRecord<String, Object, Object> deadLetter = redisTemplate.opsForStream()
                .range(options.deadLetterStreamKey(), Range.unbounded()).getFirst();
        assertThat(deadLetter.getValue().get(RedisStreamQueue.FAILURE_CODE_FIELD))
                .isEqualTo("deserialisation_failed");
        assertThat(deadLetter.getValue().get(RedisStreamQueue.ORIGINAL_MESSAGE_ID_FIELD))
                .isEqualTo(sourceId);
    }

    @Test
    void purgingRemovesBothStreamsAndAllowsTheGroupToBeRecreated() {
        RedisStreamQueueOptions options = options(Duration.ofMillis(100), 5);
        RedisStreamQueue<String> queue = stringQueue(options);
        queue.publish("active message");
        queue.deadLetter(queue.pollBatch(1).getFirst(), "invalid");
        assertThat(queue.deadLetterSize()).isEqualTo(1);

        queue.purge();

        assertThat(queue.backlogSize()).isZero();
        assertThat(queue.deadLetterSize()).isZero();
        String publishedId = queue.publish("after purge");
        QueueMessage<String> message = queue.pollBatch(1).getFirst();
        assertThat(message.messageId()).isEqualTo(publishedId);
        assertThat(message.payload()).isEqualTo("after purge");
        queue.acknowledge(message);
        assertThat(queue.backlogSize()).isZero();
    }

    @Test
    void secondConsumerCanJoinAnExistingGroup() {
        String streamKey = key();
        String group = "workers";
        RedisStreamQueueOptions firstOptions = options(streamKey, group, "consumer-one", 5);
        RedisStreamQueueOptions secondOptions = options(streamKey, group, "consumer-two", 5);
        RedisStreamQueue<String> firstQueue = stringQueue(firstOptions);
        RedisStreamQueue<String> secondQueue = stringQueue(secondOptions);
        String firstId = firstQueue.publish("first");
        String secondId = secondQueue.publish("second");

        List<QueueMessage<String>> batch = secondQueue.pollBatch(2);

        assertThat(batch).extracting(QueueMessage::messageId).containsExactly(firstId, secondId);
        batch.forEach(secondQueue::acknowledge);
        assertThat(secondQueue.backlogSize()).isZero();
    }

    private static String deadLetter(RedisStreamQueue<String> queue, String payload) {
        String id = queue.publish(payload);
        queue.deadLetter(queue.pollBatch(1).getFirst(), "rejected");
        return id;
    }

    private static RedisStreamQueue<Payload> payloadQueue(RedisStreamQueueOptions options) {
        return new RedisStreamQueue<>(redisTemplate, objectMapper,
                objectMapper.constructType(Payload.class), options);
    }

    private static RedisStreamQueue<String> stringQueue(RedisStreamQueueOptions options) {
        return new RedisStreamQueue<>(redisTemplate, objectMapper,
                objectMapper.constructType(String.class), options);
    }

    private static RedisStreamQueueOptions options(Duration reclaimMinIdle, long deadLetterMaxSize) {
        return options(key(), "workers", "consumer-" + UUID.randomUUID(), deadLetterMaxSize, reclaimMinIdle);
    }

    private static RedisStreamQueueOptions options(String streamKey, String group, String consumer, long deadLetterMaxSize) {
        return options(streamKey, group, consumer, deadLetterMaxSize, Duration.ofMillis(100));
    }

    private static RedisStreamQueueOptions options(String streamKey, String group, String consumer,
                                                    long deadLetterMaxSize, Duration reclaimMinIdle) {
        return new RedisStreamQueueOptions(streamKey, streamKey + ":dead-letter", group, consumer,
                reclaimMinIdle, Duration.ofMillis(20), deadLetterMaxSize);
    }

    private static String key() {
        return "redispatch:integration:" + UUID.randomUUID();
    }

    private record Payload(String id, String description) {
    }
}
