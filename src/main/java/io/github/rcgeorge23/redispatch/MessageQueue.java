package io.github.rcgeorge23.redispatch;

import java.util.Collection;
import java.util.List;

/**
 * A queue with at-least-once delivery. A consumer should acknowledge a message only after its
 * durable work has committed. Releasing a message leaves it pending; it is available again only
 * after the queue's stale-delivery threshold has elapsed and it is reclaimed.
 *
 * @param <T> payload type
 */
public interface MessageQueue<T> {

    /**
     * Publishes one payload.
     *
     * @param payload payload to enqueue
     * @return queue-assigned message identifier
     */
    String publish(T payload);

    /**
     * Publishes payloads in iteration order.
     *
     * @param payloads payloads to enqueue
     * @return message identifiers in the same order as the payloads
     */
    default List<String> publishAll(Collection<? extends T> payloads) {
        return payloads.stream().map(this::publish).toList();
    }

    /**
     * Delivers up to {@code limit} new messages and marks them pending for this consumer.
     *
     * @param limit maximum number of messages to return; non-positive values return an empty list
     * @return newly delivered messages
     */
    List<QueueMessage<T>> pollBatch(int limit);

    /**
     * Removes a message after its durable side effects have committed.
     *
     * @param message delivered message to acknowledge
     */
    void acknowledge(QueueMessage<T> message);

    /**
     * Leaves the message pending for stale reclamation; it does not make the message ready now.
     *
     * @param message delivered message to leave pending
     */
    void release(QueueMessage<T> message);

    /**
     * Moves a message to the bounded dead-letter queue before acknowledging it.
     *
     * @param message delivered message to dead-letter
     * @param failureCode stable, non-blank description of why processing failed
     */
    void deadLetter(QueueMessage<T> message, String failureCode);

    /**
     * Reclaims pending messages whose last delivery is older than the configured threshold.
     *
     * @param limit maximum number of stale messages to reclaim
     * @return reclaimed messages
     */
    List<QueueMessage<T>> reclaimStale(int limit);

    /**
     * Returns the number of active messages, including pending deliveries.
     *
     * @return active queue size
     */
    long backlogSize();

    /**
     * Returns the number of retained dead-letter messages.
     *
     * @return dead-letter queue size
     */
    long deadLetterSize();

    /**
     * Returns milliseconds for the oldest pending delivery, or -1 when there is no pending item.
     *
     * @return idle time in milliseconds, or -1 when there is no pending item
     */
    long oldestPendingIdleMillis();

    /**
     * Deletes active and dead-letter queue contents. Coordinate this destructive operation across consumers.
     */
    void purge();

    /**
     * Retains no more than the requested number of dead-letter messages.
     *
     * @param maxSize maximum number to retain
     */
    void trimDeadLetter(long maxSize);
}
