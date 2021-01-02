package dev.efaust.collab;

import dev.efaust.collab.Message;
import dev.efaust.collab.MessageType;

public class AcceptMessage extends Message {
    private long executionId;
    private long n;
    private long v;

    @Override
    public MessageType getMessageType() {
        return MessageType.Accept;
    }
}
