package io.github.rcgeorge23.redispatch;

import java.util.Objects;

/**
 * An immutable payload paired with its queue-assigned identifier.
 *
 * @param <T> payload type
 * @param messageId queue-assigned identifier
 * @param payload message payload
 */
public record QueueMessage<T>(String messageId, T payload) {

    /**
     * Creates a message with a non-blank identifier and non-null payload.
     *
     * @throws IllegalArgumentException if the identifier is blank
     * @throws NullPointerException if the payload is null
     */
    public QueueMessage {
        if (messageId == null || messageId.isBlank()) {
            throw new IllegalArgumentException("messageId must not be blank");
        }
        Objects.requireNonNull(payload, "payload must not be null");
    }
}
