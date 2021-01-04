package dev.efaust.collab.liveness;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.messaging.Message;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Message indicating that a node is alive and functioning.
 */
@EqualsAndHashCode(callSuper = true)
public class HeartbeatMessage extends Message {
    // TODO: consider encoding peer count in heartbeats

    @Getter @Setter
    long uuid;

    @Override
    public MessageType getMessageType() {
        return MessageType.Heartbeat;
    }

    @Override
    public String toString() {
        return String.format("<Heartbeat src='%s' uuid='%d' />", getSourceAddress(), getUuid());
    }
}
