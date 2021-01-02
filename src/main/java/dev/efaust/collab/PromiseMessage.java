package dev.efaust.collab;

import dev.efaust.collab.Message;
import dev.efaust.collab.MessageType;

public class PromiseMessage extends Message {
    private long executionId;
    private long m;
    private long w;

    @Override
    public MessageType getMessageType() {
        return MessageType.Promise;
    }
}
