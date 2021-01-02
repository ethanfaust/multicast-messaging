package dev.efaust.collab.liveness;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.messaging.Message;

public class HeartbeatMessage extends Message {
    @Override
    public MessageType getMessageType() {
        return MessageType.Heartbeat;
    }

    @Override
    public String toString() {
        return String.format("<Heartbeat src='%s' />", getSourceAddress());
    }
}
