package dev.efaust.collab;

public class HeartbeatMessage extends Message {
    @Override
    public MessageType getMessageType() {
        return MessageType.Heartbeat;
    }
}
