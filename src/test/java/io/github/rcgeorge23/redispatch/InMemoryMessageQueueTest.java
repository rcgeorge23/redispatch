package io.github.rcgeorge23.redispatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryMessageQueueTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    private InMemoryMessageQueue<String> queue;

    @BeforeEach
    void setUp() {
        queue = new InMemoryMessageQueue<>(Duration.ofSeconds(5), 2, clock);
    }

    @Test
    void publishPollAndAcknowledge() {
        String messageId = queue.publish("event");

        assertThat(messageId).isNotBlank();
        assertThat(queue.backlogSize()).isEqualTo(1);
        QueueMessage<String> message = queue.pollBatch(1).getFirst();
        assertThat(message).isEqualTo(new QueueMessage<>(messageId, "event"));

        queue.acknowledge(message);

        assertThat(queue.backlogSize()).isZero();
        assertThat(queue.oldestPendingIdleMillis()).isEqualTo(-1L);
    }

    @Test
    void pollBatchPreservesFifoOrderAndHonoursItsLimit() {
        queue.publish("first");
        queue.publish("second");
        queue.publish("third");

        assertThat(queue.pollBatch(2)).extracting(QueueMessage::payload)
                .containsExactly("first", "second");
        assertThat(queue.pollBatch(2)).extracting(QueueMessage::payload)
                .containsExactly("third");
        assertThat(queue.pollBatch(0)).isEmpty();
    }

    @Test
    void releaseKeepsMessagePendingUntilStaleReclaim() {
        queue.publish("event");
        QueueMessage<String> message = queue.pollBatch(1).getFirst();

        queue.release(message);

        assertThat(queue.pollBatch(1)).isEmpty();
        assertThat(queue.reclaimStale(10)).isEmpty();
        clock.advance(Duration.ofSeconds(5));

        assertThat(queue.reclaimStale(10)).containsExactly(message);
    }

    @Test
    void reclaimsRespectTheBatchLimitAndResetTheIdleClock() {
        queue.publish("first");
        queue.publish("second");
        List<QueueMessage<String>> originalBatch = queue.pollBatch(2);

        clock.advance(Duration.ofSeconds(5));
        assertThat(queue.reclaimStale(1)).containsExactly(originalBatch.getFirst());
        assertThat(queue.reclaimStale(1)).containsExactly(originalBatch.get(1));
        assertThat(queue.reclaimStale(2)).isEmpty();

        clock.advance(Duration.ofSeconds(5));
        assertThat(queue.reclaimStale(2)).containsExactly(originalBatch.getFirst(), originalBatch.get(1));
    }

    @Test
    void oldestPendingIdleUsesTheOldestQueueEntryAfterAnotherEntryIsReclaimed() {
        queue.publish("first");
        queue.publish("second");
        queue.pollBatch(2);
        clock.advance(Duration.ofSeconds(10));

        assertThat(queue.reclaimStale(1)).extracting(QueueMessage::payload).containsExactly("first");
        assertThat(queue.oldestPendingIdleMillis()).isZero();
    }

    @Test
    void blankFailureCodeDoesNotRemoveAnInMemoryMessage() {
        queue.publish("event");
        QueueMessage<String> message = queue.pollBatch(1).getFirst();

        assertThatThrownBy(() -> queue.deadLetter(message, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("failureCode must not be blank");

        assertThat(queue.backlogSize()).isEqualTo(1L);
        assertThat(queue.deadLetterSize()).isZero();
    }

    @Test
    void acknowledgeAndReleaseRejectNullMessages() {
        assertThatThrownBy(() -> queue.acknowledge(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("message must not be null");
        assertThatThrownBy(() -> queue.release(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("message must not be null");
    }

    @Test
    void constructorRejectsInvalidBoundsAndDependencies() {
        assertThatThrownBy(() -> new InMemoryMessageQueue<String>(Duration.ofSeconds(-1), 1, clock))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InMemoryMessageQueue<String>(Duration.ZERO, -1, clock))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InMemoryMessageQueue<String>(Duration.ZERO, 1, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("clock must not be null");
    }

    @Test
    void zeroDeadLetterLimitAcknowledgesWithoutKeepingTheFailedMessage() {
        InMemoryMessageQueue<String> zeroLimitQueue = new InMemoryMessageQueue<>(Duration.ZERO, 0, clock);
        zeroLimitQueue.publish("event");
        QueueMessage<String> message = zeroLimitQueue.pollBatch(1).getFirst();

        zeroLimitQueue.deadLetter(message, "failed");

        assertThat(zeroLimitQueue.backlogSize()).isZero();
        assertThat(zeroLimitQueue.deadLetterSize()).isZero();
    }

    @Test
    void publishAllPreservesInputOrderAndReturnsCorrespondingIds() {
        List<String> ids = queue.publishAll(List.of("first", "second"));

        assertThat(queue.pollBatch(2))
                .extracting(QueueMessage::messageId)
                .containsExactlyElementsOf(ids);
    }

    @Test
    void deadLetterIsBoundedAndCanBeTrimmed() {
        queue.publish("first");
        queue.publish("second");
        queue.publish("third");

        for (QueueMessage<String> message : queue.pollBatch(3)) {
            queue.deadLetter(message, "failed");
        }

        assertThat(queue.backlogSize()).isZero();
        assertThat(queue.deadLetterSize()).isEqualTo(2);
        queue.trimDeadLetter(1);
        assertThat(queue.deadLetterSize()).isEqualTo(1);
    }

    @Test
    void purgeClearsReadyPendingAndDeadLetterMessages() {
        queue.publish("dead-lettered");
        queue.publish("pending");
        QueueMessage<String> deadLettered = queue.pollBatch(1).getFirst();
        queue.deadLetter(deadLettered, "failed");
        queue.pollBatch(1);
        queue.publish("ready");

        queue.purge();

        assertThat(queue.backlogSize()).isZero();
        assertThat(queue.deadLetterSize()).isZero();
        assertThat(queue.oldestPendingIdleMillis()).isEqualTo(-1L);
        assertThat(queue.pollBatch(10)).isEmpty();
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now;

        private MutableClock(Instant initialTime) {
            now = new AtomicReference<>(initialTime);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }

        private void advance(Duration duration) {
            now.updateAndGet(time -> time.plus(duration));
        }
    }
}
