package dev.efaust.collab.paxos;

import dev.efaust.collab.messaging.Message;
import lombok.Getter;
import lombok.Setter;

/**
 * Base class for Paxos messages.
 */
public abstract class PaxosMessage extends Message {
    /**
     *  Multiple rounds of messages are exchanged for one consensus decision.
     *  executionId differentiates subsequent consensus decisions
     *  (e.g. think a log of transactions, this is the transactionId)
     *
     *  Paxos is used as the consensus algorithm to decide which value shows up in each slot,
     *  or which leader is elected at each time index.
     */
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