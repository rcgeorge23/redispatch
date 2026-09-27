package io.github.rcgeorge23.redispatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.RedisSystemException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class RedisStreamQueueTest {

    private StringRedisTemplate redisTemplate;
    private StreamOperations<String, Object, Object> streamOperations;
    private RedisStreamQueue<String> queue;

    @BeforeEach
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        streamOperations = mock(StreamOperations.class);
        when(redisTemplate.opsForStream()).thenReturn(streamOperations);
        when(streamOperations.add(any(MapRecord.class))).thenReturn(RecordId.of("9-0"));
        ObjectMapper objectMapper = new ObjectMapper();
        RedisStreamQueueOptions options = new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ofSeconds(30), Duration.ofMillis(100), 10);
        queue = new RedisStreamQueue<>(redisTemplate, objectMapper,
                objectMapper.constructType(String.class), options);
    }

    @Test
    void publishCreatesTheGroupAndStoresSerializedPayload() {
        when(streamOperations.add(any(MapRecord.class))).thenReturn(RecordId.of("1-0"));

        String messageId = queue.publish("hello");

        assertThat(messageId).isEqualTo("1-0");
        verify(streamOperations).createGroup("events", ReadOffset.from("0"), "workers");
        org.mockito.ArgumentCaptor<MapRecord> record = org.mockito.ArgumentCaptor.forClass(MapRecord.class);
        verify(streamOperations).add(record.capture());
        assertThat(record.getValue().getStream()).isEqualTo("events");
        assertThat(record.getValue().getValue()).isEqualTo(Map.of("payload", "\"hello\""));
    }

    @Test
    void publishRejectsNullPayloadBeforeCallingRedis() {
        assertThatThrownBy(() -> queue.publish(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("payload must not be null");

        verify(redisTemplate, never()).opsForStream();
    }

    @Test
    void publishFailsWhenRedisDoesNotReturnAnEntryId() {
        when(streamOperations.add(any(MapRecord.class))).thenReturn(null);

        assertThatThrownBy(() -> queue.publish("hello"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ID");
    }

    @Test
    void publishDoesNotWriteWhenPayloadSerializationFails() throws JsonProcessingException {
        ObjectMapper failingMapper = mock(ObjectMapper.class);
        ObjectMapper typeMapper = new ObjectMapper();
        JavaType payloadType = typeMapper.constructType(String.class);
        JsonProcessingException failure = new JsonProcessingException("cannot serialize") { };
        when(failingMapper.writeValueAsString("hello")).thenThrow(failure);
        RedisStreamQueue<String> failingQueue = new RedisStreamQueue<>(redisTemplate, failingMapper,
                payloadType, new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ofSeconds(30), Duration.ofMillis(100), 10));

        assertThatThrownBy(() -> failingQueue.publish("hello"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasCause(failure);

        verify(streamOperations, never()).add(any(MapRecord.class));
    }

    @Test
    void pollBatchDeserializesMessagesFromTheConsumerGroup() {
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(List.of(record("events", "1-0", Map.of("payload", "\"hello\""))));

        List<QueueMessage<String>> batch = queue.pollBatch(5);

        assertThat(batch).containsExactly(new QueueMessage<>("1-0", "hello"));
    }

    @Test
    void pollBatchDeserializesNestedPayloadTypes() {
        ObjectMapper objectMapper = new ObjectMapper();
        JavaType payloadType = objectMapper.getTypeFactory()
                .constructCollectionType(List.class, Integer.class);
        RedisStreamQueue<List<Integer>> typedQueue = new RedisStreamQueue<>(redisTemplate, objectMapper,
                payloadType, new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ofSeconds(30), Duration.ofMillis(100), 10));
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(List.of(record("events", "5-0", Map.of("payload", "[1,2,3]"))));

        assertThat(typedQueue.pollBatch(1).getFirst().payload()).containsExactly(1, 2, 3);
    }

    @Test
    void pollBatchTreatsANullRedisResponseAsAnEmptyBatch() {
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(null);

        assertThat(queue.pollBatch(5)).isEmpty();
    }

    @Test
    void pollBatchDoesNotAcknowledgeWhenRedisReadFails() {
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenThrow(new RedisSystemException("Redis unavailable", null));

        assertThatThrownBy(() -> queue.pollBatch(5))
                .isInstanceOf(RedisSystemException.class);

        verify(streamOperations, never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        verify(streamOperations, never()).delete(anyString(), any(RecordId.class));
    }

    @Test
    void pollBatchKeepsValidMessagesAndDeadLettersMalformedOnes() {
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(List.of(
                        record("events", "1-0", Map.of("payload", "\"valid\"")),
                        record("events", "2-0", Map.of("payload", "not-json"))));

        assertThat(queue.pollBatch(5)).containsExactly(new QueueMessage<>("1-0", "valid"));

        org.mockito.ArgumentCaptor<MapRecord> deadLetter = org.mockito.ArgumentCaptor.forClass(MapRecord.class);
        verify(streamOperations).add(deadLetter.capture());
        assertThat(((Map<?, ?>) deadLetter.getValue().getValue()).get("originalMessageId")).isEqualTo("2-0");
        verify(streamOperations).acknowledge("events", "workers", RecordId.of("2-0"));
        verify(streamOperations).delete("events", RecordId.of("2-0"));
        verify(streamOperations, never()).acknowledge("events", "workers", RecordId.of("1-0"));
    }

    @Test
    void nonPositiveBatchLimitsDoNotContactRedis() {
        assertThat(queue.pollBatch(0)).isEmpty();
        assertThat(queue.pollBatch(-1)).isEmpty();

        verify(redisTemplate, never()).opsForStream();
    }

    @Test
    void acknowledgeRemovesTheMessageFromItsConsumerGroupAndStream() {
        queue.acknowledge(new QueueMessage<>("1-0", "hello"));

        var order = inOrder(streamOperations);
        order.verify(streamOperations).acknowledge("events", "workers", RecordId.of("1-0"));
        order.verify(streamOperations).delete("events", RecordId.of("1-0"));
    }

    @Test
    void acknowledgeRejectsNullMessagesBeforeCallingRedis() {
        assertThatThrownBy(() -> queue.acknowledge(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("message must not be null");

        verify(redisTemplate, never()).opsForStream();
    }

    @Test
    void acknowledgementFailureDoesNotDeleteTheStreamEntry() {
        when(streamOperations.acknowledge("events", "workers", RecordId.of("1-0")))
                .thenThrow(new RedisSystemException("Redis unavailable", null));

        assertThatThrownBy(() -> queue.acknowledge(new QueueMessage<>("1-0", "hello")))
                .isInstanceOf(RedisSystemException.class);

        verify(streamOperations, never()).delete(anyString(), any(RecordId.class));
    }

    @Test
    void releaseLeavesTheMessagePendingWithoutAcknowledgingIt() {
        queue.release(new QueueMessage<>("1-0", "hello"));

        verify(redisTemplate, never()).opsForStream();
    }

    @Test
    void releaseRejectsNullMessages() {
        assertThatThrownBy(() -> queue.release(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("message must not be null");
    }

    @Test
    void deadLetterStoresPayloadReasonAndOriginalIdBeforeAcknowledging() {
        QueueMessage<String> message = new QueueMessage<>("1-0", "hello");
        queue.deadLetter(message, "handler_failed");

        var order = inOrder(streamOperations);
        org.mockito.ArgumentCaptor<MapRecord> record = org.mockito.ArgumentCaptor.forClass(MapRecord.class);
        order.verify(streamOperations).add(record.capture());
        assertThat(record.getValue().getStream()).isEqualTo("events:dead-letter");
        assertThat(record.getValue().getValue()).isEqualTo(Map.of(
                "payload", "\"hello\"",
                "failureCode", "handler_failed",
                "originalMessageId", "1-0"));
        order.verify(streamOperations).trim("events:dead-letter", 10L);
        order.verify(streamOperations).acknowledge("events", "workers", RecordId.of("1-0"));
        order.verify(streamOperations).delete("events", RecordId.of("1-0"));
    }

    @Test
    void deadLetterDoesNotAcknowledgeIfRedisDidNotStoreTheDeadLetter() {
        when(streamOperations.add(any(MapRecord.class))).thenReturn(null);

        assertThatThrownBy(() -> queue.deadLetter(new QueueMessage<>("1-0", "hello"), "handler_failed"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dead-letter");

        verify(streamOperations, never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        verify(streamOperations, never()).delete(anyString(), any(RecordId.class));
        verify(streamOperations, never()).trim(anyString(), anyLong());
    }

    @Test
    void deadLetterDoesNotAcknowledgeWhenRedisWriteThrows() {
        when(streamOperations.add(any(MapRecord.class)))
                .thenThrow(new RedisSystemException("Redis unavailable", null));

        assertThatThrownBy(() -> queue.deadLetter(new QueueMessage<>("1-0", "hello"), "handler_failed"))
                .isInstanceOf(RedisSystemException.class);

        verify(streamOperations, never()).trim(anyString(), anyLong());
        verify(streamOperations, never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        verify(streamOperations, never()).delete(anyString(), any(RecordId.class));
    }

    @Test
    void deadLetterRejectsBlankFailureCodesBeforeCallingRedis() {
        assertThatThrownBy(() -> queue.deadLetter(new QueueMessage<>("1-0", "hello"), "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("failureCode must not be blank");

        verify(redisTemplate, never()).opsForStream();
    }

    @Test
    void deadLetterRejectsNullMessagesBeforeCallingRedis() {
        assertThatThrownBy(() -> queue.deadLetter(null, "handler_failed"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("message must not be null");

        verify(redisTemplate, never()).opsForStream();
    }

    @Test
    void deadLetterDoesNotAcknowledgeIfTrimmingFails() {
        when(streamOperations.trim("events:dead-letter", 10L))
                .thenThrow(new RedisSystemException("Redis unavailable", null));

        assertThatThrownBy(() -> queue.deadLetter(new QueueMessage<>("1-0", "hello"), "handler_failed"))
                .isInstanceOf(RedisSystemException.class);

        verify(streamOperations, never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        verify(streamOperations, never()).delete(anyString(), any(RecordId.class));
    }

    @Test
    void deadLetterDoesNotAcknowledgeWhenPayloadSerializationFails() throws JsonProcessingException {
        ObjectMapper failingMapper = mock(ObjectMapper.class);
        ObjectWriter failingWriter = mock(ObjectWriter.class);
        ObjectMapper typeMapper = new ObjectMapper();
        JavaType payloadType = typeMapper.constructType(String.class);
        JsonProcessingException failure = new JsonProcessingException("cannot serialize") { };
        when(failingMapper.writerFor(payloadType)).thenReturn(failingWriter);
        when(failingWriter.writeValueAsString("hello")).thenThrow(failure);
        RedisStreamQueue<String> failingQueue = new RedisStreamQueue<>(redisTemplate, failingMapper,
                payloadType, new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ofSeconds(30), Duration.ofMillis(100), 10));

        assertThatThrownBy(() -> failingQueue.deadLetter(new QueueMessage<>("1-0", "hello"), "handler_failed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasCause(failure);

        verify(streamOperations, never()).add(any(MapRecord.class));
        verify(streamOperations, never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        verify(streamOperations, never()).delete(anyString(), any(RecordId.class));
    }

    @Test
    void zeroDeadLetterLimitDiscardsTheDeadLetterButAcknowledgesTheOriginal() {
        ObjectMapper objectMapper = new ObjectMapper();
        RedisStreamQueueOptions zeroLimitOptions = new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ofSeconds(30), Duration.ofMillis(100), 0);
        RedisStreamQueue<String> zeroLimitQueue = new RedisStreamQueue<>(
                redisTemplate, objectMapper, objectMapper.constructType(String.class), zeroLimitOptions);
        when(streamOperations.add(any(MapRecord.class))).thenReturn(RecordId.of("2-0"));

        zeroLimitQueue.deadLetter(new QueueMessage<>("1-0", "hello"), "handler_failed");

        verify(streamOperations).trim("events:dead-letter", 0L);
        verify(streamOperations).acknowledge("events", "workers", RecordId.of("1-0"));
        verify(streamOperations).delete("events", RecordId.of("1-0"));
    }

    @Test
    void malformedPayloadIsDeadLetteredAndRemovedFromTheActiveStream() {
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(List.of(record("events", "2-0", Map.of("payload", "not-json"))));

        assertThat(queue.pollBatch(5)).isEmpty();

        org.mockito.ArgumentCaptor<MapRecord> record = org.mockito.ArgumentCaptor.forClass(MapRecord.class);
        verify(streamOperations).add(record.capture());
        assertThat(record.getValue().getValue()).isEqualTo(Map.of(
                "payload", "not-json",
                "failureCode", "deserialisation_failed",
                "originalMessageId", "2-0"));
        verify(streamOperations).acknowledge("events", "workers", RecordId.of("2-0"));
        verify(streamOperations).delete("events", RecordId.of("2-0"));
    }

    @Test
    void malformedRecordWithoutPayloadIsDeadLetteredWithAnEmptyPayload() {
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(List.of(record("events", "3-0", Map.of("other", "field"))));

        assertThat(queue.pollBatch(1)).isEmpty();

        org.mockito.ArgumentCaptor<MapRecord> deadLetter = org.mockito.ArgumentCaptor.forClass(MapRecord.class);
        verify(streamOperations).add(deadLetter.capture());
        assertThat(deadLetter.getValue().getValue()).isEqualTo(Map.of(
                "payload", "",
                "failureCode", "deserialisation_failed",
                "originalMessageId", "3-0"));
        verify(streamOperations).acknowledge("events", "workers", RecordId.of("3-0"));
        verify(streamOperations).delete("events", RecordId.of("3-0"));
    }

    @Test
    void recordWithANullFieldMapIsDeadLetteredWithoutLosingItsId() {
        MapRecord malformed = mock(MapRecord.class);
        when(malformed.getId()).thenReturn(RecordId.of("5-0"));
        when(malformed.getValue()).thenReturn(null);
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(List.of(malformed));

        assertThat(queue.pollBatch(1)).isEmpty();

        org.mockito.ArgumentCaptor<MapRecord> deadLetter = org.mockito.ArgumentCaptor.forClass(MapRecord.class);
        verify(streamOperations).add(deadLetter.capture());
        assertThat(deadLetter.getValue().getValue()).isEqualTo(Map.of(
                "payload", "",
                "failureCode", "deserialisation_failed",
                "originalMessageId", "5-0"));
        verify(streamOperations).acknowledge("events", "workers", RecordId.of("5-0"));
        verify(streamOperations).delete("events", RecordId.of("5-0"));
    }

    @Test
    void recordWithoutAnIdFailsClosedWithoutAcknowledgingIt() {
        MapRecord malformed = mock(MapRecord.class);
        when(malformed.getId()).thenReturn(null);
        when(malformed.getValue()).thenReturn(Map.of("payload", "\"hello\""));
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(List.of(malformed));

        assertThatThrownBy(() -> queue.pollBatch(1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("record ID must not be null");

        verify(streamOperations, never()).add(any(MapRecord.class));
        verify(streamOperations, never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        verify(streamOperations, never()).delete(anyString(), any(RecordId.class));
    }

    @Test
    void jsonNullPayloadIsDeadLetteredInsteadOfReturnedAsAMessage() {
        when(streamOperations.read(any(Consumer.class), any(StreamReadOptions.class), any(StreamOffset.class)))
                .thenReturn(List.of(record("events", "4-0", Map.of("payload", "null"))));

        assertThat(queue.pollBatch(1)).isEmpty();

        verify(streamOperations).acknowledge("events", "workers", RecordId.of("4-0"));
        verify(streamOperations).delete("events", RecordId.of("4-0"));
    }

    @Test
    void reclaimStaleClaimsOnlyMessagesPastTheIdleThreshold() {
        PendingMessage tooYoung = pending("1-0", Duration.ofSeconds(2));
        PendingMessage stale = pending("2-0", Duration.ofSeconds(30));
        when(streamOperations.pending(anyString(), anyString(), any(Range.class), anyLong(), any(Duration.class)))
                .thenReturn(new PendingMessages("workers", List.of(tooYoung, stale)));
        when(streamOperations.claim(anyString(), anyString(), anyString(), any(Duration.class), any(RecordId.class)))
                .thenReturn(List.of(record("events", "2-0", Map.of("payload", "\"retry\""))));

        assertThat(queue.reclaimStale(2)).containsExactly(new QueueMessage<>("2-0", "retry"));

        verify(streamOperations, never()).claim(
                "events", "workers", "worker-1", Duration.ofSeconds(30), RecordId.of("1-0"));
        verify(streamOperations).claim(
                "events", "workers", "worker-1", Duration.ofSeconds(30), RecordId.of("2-0"));
    }

    @Test
    void reclaimAsksRedisToFilterFreshPendingMessagesBeforeApplyingItsBatchLimit() {
        PendingMessage laterStaleMessage = pending("3-0", Duration.ofSeconds(30));
        when(streamOperations.pending(eq("events"), eq("workers"), any(Range.class), eq(1L),
                eq(Duration.ofSeconds(30))))
                .thenReturn(new PendingMessages("workers", List.of(laterStaleMessage)));
        when(streamOperations.claim(anyString(), anyString(), anyString(), any(Duration.class), any(RecordId.class)))
                .thenReturn(List.of(record("events", "3-0", Map.of("payload", "\"retry\""))));

        assertThat(queue.reclaimStale(1)).containsExactly(new QueueMessage<>("3-0", "retry"));

        verify(streamOperations).pending(
                eq("events"), eq("workers"), any(Range.class), eq(1L), eq(Duration.ofSeconds(30)));
        verify(streamOperations).claim(
                "events", "workers", "worker-1", Duration.ofSeconds(30), RecordId.of("3-0"));
    }

    @Test
    void reclaimWithNonPositiveLimitDoesNotContactRedis() {
        assertThat(queue.reclaimStale(0)).isEmpty();
        assertThat(queue.reclaimStale(-1)).isEmpty();

        verify(redisTemplate, never()).opsForStream();
    }

    @Test
    void reclaimHandlesMissingPendingResultsAndMessagesClaimedByAnotherConsumer() {
        when(streamOperations.pending(anyString(), anyString(), any(Range.class), anyLong(), any(Duration.class)))
                .thenReturn(null)
                .thenReturn(new PendingMessages("workers", List.of(pending("1-0", Duration.ofSeconds(30)))));
        when(streamOperations.claim(anyString(), anyString(), anyString(), any(Duration.class), any(RecordId.class)))
                .thenReturn(null);

        assertThat(queue.reclaimStale(1)).isEmpty();
        assertThat(queue.reclaimStale(1)).isEmpty();

        verify(streamOperations).claim(
                "events", "workers", "worker-1", Duration.ofSeconds(30), RecordId.of("1-0"));
    }

    @Test
    void reclaimDoesNotAcknowledgeWhenClaimFails() {
        PendingMessage staleMessage = pending("1-0", Duration.ofSeconds(30));
        when(streamOperations.pending(anyString(), anyString(), any(Range.class), anyLong(), any(Duration.class)))
                .thenReturn(new PendingMessages("workers", List.of(staleMessage)));
        when(streamOperations.claim(anyString(), anyString(), anyString(), any(Duration.class), any(RecordId.class)))
                .thenThrow(new RedisSystemException("Redis unavailable", null));

        assertThatThrownBy(() -> queue.reclaimStale(1))
                .isInstanceOf(RedisSystemException.class);

        verify(streamOperations, never()).acknowledge(anyString(), anyString(), any(RecordId.class));
        verify(streamOperations, never()).delete(anyString(), any(RecordId.class));
    }

    @Test
    void reclaimDeadLettersMalformedClaimedMessages() {
        PendingMessage staleMessage = pending("1-0", Duration.ofSeconds(30));
        when(streamOperations.pending(anyString(), anyString(), any(Range.class), anyLong(), any(Duration.class)))
                .thenReturn(new PendingMessages("workers", List.of(staleMessage)));
        when(streamOperations.claim(anyString(), anyString(), anyString(), any(Duration.class), any(RecordId.class)))
                .thenReturn(List.of(record("events", "1-0", Map.of("payload", "not-json"))));

        assertThat(queue.reclaimStale(1)).isEmpty();

        org.mockito.ArgumentCaptor<MapRecord> deadLetter = org.mockito.ArgumentCaptor.forClass(MapRecord.class);
        verify(streamOperations).add(deadLetter.capture());
        assertThat(((Map<?, ?>) deadLetter.getValue().getValue()).get("originalMessageId")).isEqualTo("1-0");
        verify(streamOperations).acknowledge("events", "workers", RecordId.of("1-0"));
        verify(streamOperations).delete("events", RecordId.of("1-0"));
    }

    @Test
    void diagnosticsReportStreamSizesAndOldestPendingIdleTime() {
        PendingMessage oldest = pending("1-0", Duration.ofMillis(456));
        when(streamOperations.size("events")).thenReturn(3L);
        when(streamOperations.size("events:dead-letter")).thenReturn(2L);
        when(streamOperations.pending(anyString(), anyString(), any(Range.class), anyLong()))
                .thenReturn(new PendingMessages("workers", List.of(oldest)));

        assertThat(queue.backlogSize()).isEqualTo(3L);
        assertThat(queue.deadLetterSize()).isEqualTo(2L);
        assertThat(queue.oldestPendingIdleMillis()).isEqualTo(456L);
    }

    @Test
    void emptyDiagnosticsReturnZeroAndMinusOne() {
        when(streamOperations.size(anyString())).thenReturn(null);
        when(streamOperations.pending(anyString(), anyString(), any(Range.class), anyLong()))
                .thenReturn(new PendingMessages("workers", List.of()));

        assertThat(queue.backlogSize()).isZero();
        assertThat(queue.deadLetterSize()).isZero();
        assertThat(queue.oldestPendingIdleMillis()).isEqualTo(-1L);
    }

    @Test
    void purgeDeletesBothStreamsAndRecreatesTheGroupBeforeReturning() {
        when(streamOperations.add(any(MapRecord.class))).thenReturn(RecordId.of("1-0"));

        queue.publish("before");
        queue.purge();
        verify(streamOperations, org.mockito.Mockito.times(2))
                .createGroup("events", ReadOffset.from("0"), "workers");
        queue.publish("after");

        verify(redisTemplate).delete(List.of("events", "events:dead-letter"));
        verify(streamOperations, org.mockito.Mockito.times(2))
                .createGroup("events", ReadOffset.from("0"), "workers");
    }

    @Test
    void anotherQueueInstanceToleratesTheConsumerGroupCreatedByItsPeer() {
        ObjectMapper objectMapper = new ObjectMapper();
        RedisStreamQueueOptions otherConsumerOptions = new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-2",
                Duration.ofSeconds(30), Duration.ofMillis(100), 10);
        RedisStreamQueue<String> otherQueue = new RedisStreamQueue<>(
                redisTemplate, objectMapper, objectMapper.constructType(String.class), otherConsumerOptions);
        when(streamOperations.createGroup(anyString(), any(ReadOffset.class), anyString()))
                .thenThrow(new RuntimeException("BUSYGROUP Consumer Group name already exists"));
        when(streamOperations.add(any(MapRecord.class))).thenReturn(RecordId.of("2-0"));

        assertThat(otherQueue.publish("hello")).isEqualTo("2-0");

        verify(streamOperations).createGroup("events", ReadOffset.from("0"), "workers");
    }

    @Test
    void optionsRejectInvalidKeysDurationsAndDeadLetterBounds() {
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                " ", "events:dead-letter", "workers", "worker-1",
                Duration.ofSeconds(1), Duration.ofSeconds(1), 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                "events", "events", "workers", "worker-1",
                Duration.ofSeconds(1), Duration.ofSeconds(1), 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ofSeconds(-1), Duration.ofSeconds(1), 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ZERO, Duration.ZERO, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ZERO, Duration.ofSeconds(1), -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                "events", "events:dead-letter", " ", "worker-1",
                Duration.ZERO, Duration.ofSeconds(1), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", null,
                Duration.ZERO, Duration.ofSeconds(1), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                null, Duration.ofSeconds(1), 1))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RedisStreamQueueOptions(
                "events", "events:dead-letter", "workers", "worker-1",
                Duration.ZERO, null, 1))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void defaultsUseACompanionDeadLetterStreamAndUniqueConsumerNames() {
        RedisStreamQueueOptions first = RedisStreamQueueOptions.defaults("jobs", "workers");
        RedisStreamQueueOptions second = RedisStreamQueueOptions.defaults("jobs", "workers");

        assertThat(first.streamKey()).isEqualTo("jobs");
        assertThat(first.deadLetterStreamKey()).isEqualTo("jobs:dead-letter");
        assertThat(first.consumerGroup()).isEqualTo("workers");
        assertThat(first.consumerName()).isNotEqualTo(second.consumerName());
        assertThat(first.reclaimMinIdle()).isEqualTo(Duration.ofSeconds(30));
        assertThat(first.readBlockTimeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(first.deadLetterMaxSize()).isEqualTo(1_000);
    }

    @Test
    void existingConsumerGroupIsTolerated() {
        AtomicInteger createCalls = new AtomicInteger();
        when(streamOperations.createGroup(anyString(), any(ReadOffset.class), anyString()))
                .thenAnswer(invocation -> {
                    createCalls.incrementAndGet();
                    throw new RuntimeException("BUSYGROUP Consumer Group name already exists");
                });
        when(streamOperations.add(any(MapRecord.class))).thenReturn(RecordId.of("1-0"));

        assertThat(queue.publish("hello")).isEqualTo("1-0");
        assertThat(queue.publish("again")).isEqualTo("1-0");
        assertThat(createCalls).hasValue(1);
    }

    @Test
    void nonBusyGroupCreationFailuresArePropagatedAndRetried() {
        AtomicInteger createCalls = new AtomicInteger();
        when(streamOperations.createGroup(anyString(), any(ReadOffset.class), anyString()))
                .thenAnswer(invocation -> {
                    createCalls.incrementAndGet();
                    throw new RuntimeException("connection refused");
                });

        assertThatThrownBy(() -> queue.publish("hello")).hasMessage("connection refused");
        assertThatThrownBy(() -> queue.publish("hello")).hasMessage("connection refused");
        assertThat(createCalls).hasValue(2);
    }

    @Test
    void trimDeadLetterRejectsNegativeSize() {
        assertThatThrownBy(() -> queue.trimDeadLetter(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @SuppressWarnings("unchecked")
    private static PendingMessage pending(String id, Duration idle) {
        PendingMessage pending = mock(PendingMessage.class);
        when(pending.getIdAsString()).thenReturn(id);
        when(pending.getElapsedTimeSinceLastDelivery()).thenReturn(idle);
        return pending;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static MapRecord<String, Object, Object> record(String stream, String id, Map<String, String> value) {
        return (MapRecord) MapRecord.create(stream, value).withId(RecordId.of(id));
    }
}
