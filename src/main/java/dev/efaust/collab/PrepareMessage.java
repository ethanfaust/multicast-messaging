package dev.efaust.collab;

import dev.efaust.collab.Message;
import dev.efaust.collab.MessageType;

public class PrepareMessage extends Message {
    private long executionId;
    private long n;

    @Override
    public MessageType getMessageType() {
        return MessageType.Prepare;
    }

    public long getExecutionId() {
        return executionId;
    }

    public void setExecutionId(long executionId) {
        this.executionId = executionId;
    }
}
