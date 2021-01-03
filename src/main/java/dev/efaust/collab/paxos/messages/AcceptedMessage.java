package dev.efaust.collab.paxos.messages;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode(callSuper = true)
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
