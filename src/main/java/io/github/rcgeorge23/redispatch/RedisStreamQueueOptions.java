package io.github.rcgeorge23.redispatch;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Configuration for one Redis Streams queue and its consumer group.
 *
 * @param streamKey active Redis Stream key
 * @param deadLetterStreamKey bounded dead-letter Redis Stream key
 * @param consumerGroup consumer group shared by workers
 * @param consumerName unique name for this worker instance
 * @param reclaimMinIdle minimum pending idle time before a message can be reclaimed
 * @param readBlockTimeout maximum time a blocking poll waits for new messages
 * @param deadLetterMaxSize maximum retained dead-letter entries
 */
public record RedisStreamQueueOptions(
        String streamKey,
        String deadLetterStreamKey,
        String consumerGroup,
        String consumerName,
        Duration reclaimMinIdle,
        Duration readBlockTimeout,
        long deadLetterMaxSize) {

    /**
     * Validates stream identity and timing/retention bounds.
     *
     * @throws IllegalArgumentException if names or bounds are invalid
     * @throws NullPointerException if a duration is null
     */
    public RedisStreamQueueOptions {
        requireText(streamKey, "streamKey");
        requireText(deadLetterStreamKey, "deadLetterStreamKey");
        requireText(consumerGroup, "consumerGroup");
        requireText(consumerName, "consumerName");
        Objects.requireNonNull(reclaimMinIdle, "reclaimMinIdle must not be null");
        Objects.requireNonNull(readBlockTimeout, "readBlockTimeout must not be null");
        if (streamKey.equals(deadLetterStreamKey)) {
            throw new IllegalArgumentException("The active and dead-letter stream keys must differ");
        }
        if (reclaimMinIdle.isNegative()) {
            throw new IllegalArgumentException("reclaimMinIdle must not be negative");
        }
        if (readBlockTimeout.isNegative() || readBlockTimeout.isZero()) {
            throw new IllegalArgumentException("readBlockTimeout must be positive");
        }
        if (deadLetterMaxSize < 0) {
            throw new IllegalArgumentException("deadLetterMaxSize must not be negative");
        }
    }

    /**
     * Creates conventional companion keys and a unique consumer name.
     *
     * @param streamKey active Redis Stream key
     * @param consumerGroup consumer group shared by workers
     * @return options with a 30-second reclaim threshold, one-second poll block and 1,000-entry DLQ
     */
    public static RedisStreamQueueOptions defaults(String streamKey, String consumerGroup) {
        return new RedisStreamQueueOptions(
                streamKey,
                streamKey + ":dead-letter",
                consumerGroup,
                "consumer-" + UUID.randomUUID(),
                Duration.ofSeconds(30),
                Duration.ofSeconds(1),
                1_000);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
