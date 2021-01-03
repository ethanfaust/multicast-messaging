package dev.efaust.collab.paxos.messages;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

// Negative accept response, indicating that the node has already promised not to accept this value of N
@EqualsAndHashCode(callSuper = true)
public class NegativePromiseMessage extends PaxosMessage {
    @Getter @Setter
    private long proposalNumber;

    @Getter @Setter
    private long priorPromisedProposalNumber;

    @Getter @Setter
    private long priorAcceptedProposalNumber;

    @Getter @Setter
    private long priorAcceptedValue;

    @Override
    public MessageType getMessageType() {
        return MessageType.NegativePromise;
    }

    @Override
    public String toString() {
        return String.format("<NegativePromise src='%s' executionId='%d', N='%d' priorPromisedN='%d' priorAcceptedN='%d' priorAcceptedValue='%d' />",
                getSourceAddress(), getExecutionId(), getProposalNumber(), getPriorPromisedProposalNumber(), getPriorAcceptedProposalNumber(), getPriorAcceptedValue());
    }
}
