package dev.efaust.collab;

public class PromiseMessage extends Message {
    @Override
    public MessageType getMessageType() {
        return MessageType.Promise;
    }
}
