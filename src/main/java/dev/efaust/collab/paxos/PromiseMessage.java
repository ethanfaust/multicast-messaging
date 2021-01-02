package dev.efaust.collab.paxos;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.Getter;
import lombok.Setter;

public class PromiseMessage extends PaxosMessage {
    @Getter @Setter
    private long promiseN;

    @Getter @Setter
    private long priorAcceptedN;

    @Getter @Setter
    private long priorAcceptedValue;

    public PromiseMessage() {
        super();
    }

    public PromiseMessage(long executionId, long promiseN, long priorAcceptedN, long priorAcceptedValue) {
        super(executionId);
        this.promiseN = promiseN;
        this.priorAcceptedN = priorAcceptedN;
        this.priorAcceptedValue = priorAcceptedValue;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.Promise;
    }

    @Override
    public String toString() {
        return String.format("<Promise src='%s' executionId='%d', promiseN='%d' priorAcceptedN='%d' priorAcceptedValue='%d' />",
                getSourceAddress(), getExecutionId(), getPromiseN(), getPriorAcceptedN(), getPriorAcceptedValue());
    }
}
