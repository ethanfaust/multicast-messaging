package dev.efaust.collab.paxos;

import dev.efaust.collab.messaging.Message;
import lombok.Getter;
import lombok.Setter;

public abstract class PaxosMessage extends Message {
    @Getter @Setter
    private long executionId;

    public PaxosMessage() {
        super();
    }

    public PaxosMessage(long executionId) {
        super();
        this.executionId = executionId;
    }
}