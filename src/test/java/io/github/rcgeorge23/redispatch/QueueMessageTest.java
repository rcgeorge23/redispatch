package io.github.rcgeorge23.redispatch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueueMessageTest {

    @Test
    void preservesTheQueueAssignedIdAndPayload() {
        QueueMessage<String> message = new QueueMessage<>("1712345678901-0", "event");

        assertThat(message.messageId()).isEqualTo("1712345678901-0");
        assertThat(message.payload()).isEqualTo("event");
    }

    @Test
    void rejectsBlankIds() {
        assertThatThrownBy(() -> new QueueMessage<String>(null, "event"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messageId must not be blank");
        assertThatThrownBy(() -> new QueueMessage<>("  ", "event"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messageId must not be blank");
    }

    @Test
    void rejectsNullPayloads() {
        assertThatThrownBy(() -> new QueueMessage<String>("1-0", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("payload must not be null");
    }
}
