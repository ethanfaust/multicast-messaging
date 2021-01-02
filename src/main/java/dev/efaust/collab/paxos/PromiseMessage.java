package dev.efaust.collab.paxos;

import dev.efaust.collab.MessageType;
import lombok.Getter;
import lombok.Setter;

/**
 * With a promise message, a node guarantees that it will not accept future proposals with number < N
 * (promiseProposalNumber).
 */
public class PromiseMessage extends PaxosMessage {
    // TODO: fix this: currently these sentinel values are part of the field
    public static final long NO_PRIOR_ACCEPTED_N = -1;
    public static final long NO_PRIOR_ACCEPTED_VALUE = -1;

    @Getter @Setter
    private long promiseProposalNumber;

    @Getter @Setter
    private long priorAcceptedProposalNumber;

    @Getter @Setter
    private long priorAcceptedValue;

    public PromiseMessage() {
        super();
    }

    public PromiseMessage(long executionId, long promiseProposalNumber, long priorAcceptedProposalNumber, long priorAcceptedValue) {
        super(executionId);
        this.promiseProposalNumber = promiseProposalNumber;
        this.priorAcceptedProposalNumber = priorAcceptedProposalNumber;
        this.priorAcceptedValue = priorAcceptedValue;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.Promise;
    }

    @Override
    public String toString() {
        return String.format("<Promise src='%s' executionId='%d', promiseN='%d' priorAcceptedN='%d' priorAcceptedValue='%d' />",
                getSourceAddress(), getExecutionId(), getPromiseProposalNumber(), getPriorAcceptedProposalNumber(), getPriorAcceptedValue());
    }
}
