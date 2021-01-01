package dev.efaust.collab;

public class PrepareMessage extends Message {
    @Override
    public MessageType getMessageType() {
        return MessageType.Prepare;
    }
}
