package io.github.rcgeorge23.redispatch;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Thread-safe in-memory adapter with the same pending and stale-reclaim behavior as Redis.
 *
 * @param <T> payload type
 */
public final class InMemoryMessageQueue<T> implements MessageQueue<T> {

    private final Deque<QueueMessage<T>> ready = new ArrayDeque<>();
    private final Map<String, Pending<T>> pending = new LinkedHashMap<>();
    private final Deque<DeadLetter<T>> deadLetters = new ArrayDeque<>();
    private final Duration reclaimMinIdle;
    private final long deadLetterMaxSize;
    private final Clock clock;

    /**
     * Creates an adapter using the system UTC clock.
     *
     * @param reclaimMinIdle minimum pending idle time before reclaim
     * @param deadLetterMaxSize maximum retained dead-letter entries
     */
    public InMemoryMessageQueue(Duration reclaimMinIdle, long deadLetterMaxSize) {
        this(reclaimMinIdle, deadLetterMaxSize, Clock.systemUTC());
    }

    /**
     * Creates an adapter with an injectable clock for deterministic stale-delivery timing.
     *
     * @param reclaimMinIdle minimum pending idle time before reclaim
     * @param deadLetterMaxSize maximum retained dead-letter entries
     * @param clock source of current time
     */
    public InMemoryMessageQueue(Duration reclaimMinIdle, long deadLetterMaxSize, Clock clock) {
        this.reclaimMinIdle = Objects.requireNonNull(reclaimMinIdle, "reclaimMinIdle must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        if (reclaimMinIdle.isNegative()) {
            throw new IllegalArgumentException("reclaimMinIdle must not be negative");
        }
        if (deadLetterMaxSize < 0) {
            throw new IllegalArgumentException("deadLetterMaxSize must not be negative");
        }
        this.deadLetterMaxSize = deadLetterMaxSize;
    }

    @Override
    public synchronized String publish(T payload) {
        QueueMessage<T> message = new QueueMessage<>(UUID.randomUUID().toString(), payload);
        ready.addLast(message);
        return message.messageId();
    }

    @Override
    public synchronized List<QueueMessage<T>> pollBatch(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<QueueMessage<T>> batch = new ArrayList<>(Math.min(limit, ready.size()));
        QueueMessage<T> message;
        Instant now = clock.instant();
        while (batch.size() < limit && (message = ready.pollFirst()) != null) {
            pending.put(message.messageId(), new Pending<>(message, now));
            batch.add(message);
        }
        return List.copyOf(batch);
    }

    @Override
    public synchronized void acknowledge(QueueMessage<T> message) {
        Objects.requireNonNull(message, "message must not be null");
        pending.remove(message.messageId());
    }

    @Override
    public synchronized void release(QueueMessage<T> message) {
        Objects.requireNonNull(message, "message must not be null");
        // Keep it pending so retry timing matches the Redis Stream adapter.
    }

    @Override
    public synchronized void deadLetter(QueueMessage<T> message, String failureCode) {
        Objects.requireNonNull(message, "message must not be null");
        if (failureCode == null || failureCode.isBlank()) {
            throw new IllegalArgumentException("failureCode must not be blank");
        }
        pending.remove(message.messageId());
        if (deadLetterMaxSize == 0) {
            return;
        }
        deadLetters.addLast(new DeadLetter<>(message, failureCode, clock.instant()));
        while (deadLetters.size() > deadLetterMaxSize) {
            deadLetters.removeFirst();
        }
    }

    @Override
    public synchronized List<QueueMessage<T>> reclaimStale(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        Instant now = clock.instant();
        List<QueueMessage<T>> reclaimed = new ArrayList<>(Math.min(limit, pending.size()));
        for (Map.Entry<String, Pending<T>> entry : pending.entrySet()) {
            if (reclaimed.size() == limit) {
                break;
            }
            Pending<T> delivery = entry.getValue();
            if (Duration.between(delivery.lastDeliveredAt(), now).compareTo(reclaimMinIdle) >= 0) {
                entry.setValue(new Pending<>(delivery.message(), now));
                reclaimed.add(delivery.message());
            }
        }
        return List.copyOf(reclaimed);
    }

    @Override
    public synchronized long backlogSize() {
        return (long) ready.size() + pending.size();
    }

    @Override
    public synchronized long deadLetterSize() {
        return deadLetters.size();
    }

    @Override
    public synchronized long oldestPendingIdleMillis() {
        Pending<T> oldestPending = pending.values().stream().findFirst().orElse(null);
        return oldestPending == null ? -1L : Math.max(
                0L, Duration.between(oldestPending.lastDeliveredAt(), clock.instant()).toMillis());
    }

    @Override
    public synchronized void purge() {
        ready.clear();
        pending.clear();
        deadLetters.clear();
    }

    @Override
    public synchronized void trimDeadLetter(long maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize must not be negative");
        }
        while (deadLetters.size() > maxSize) {
            deadLetters.removeFirst();
        }
    }

    private record Pending<T>(QueueMessage<T> message, Instant lastDeliveredAt) {
    }

    private record DeadLetter<T>(QueueMessage<T> message, String failureCode, Instant createdAt) {
    }
}
