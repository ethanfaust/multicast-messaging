package dev.efaust.collab.paxos;

import dev.efaust.collab.MessageType;
import lombok.Getter;
import lombok.Setter;

public class AcceptedMessage extends PaxosMessage {
    @Getter @Setter
    private long acceptedProposalNumber;

    @Getter @Setter
    private long acceptedValue;

    @Override
    public MessageType getMessageType() {
        return MessageType.Accepted;
    }

    @Override
    public String toString() {
        return String.format("<Accepted src='%s' executionId='%d', N='%d' V='%d' />",
                getSourceAddress(), getExecutionId(), getAcceptedProposalNumber(), getAcceptedValue());
    }
}
