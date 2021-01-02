package dev.efaust.collab;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.Message;

public class AcceptedMessage extends Message {
    private long executionId;
    private long n;
    private long v;

    @Override
    public MessageType getMessageType() {
        return MessageType.Accepted;
    }
}
