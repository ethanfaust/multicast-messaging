package dev.efaust.collab.paxos.messages;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode(callSuper = true)
public class PrepareMessage extends PaxosMessage {
    @Getter @Setter
    private long proposalNumber;

    public PrepareMessage() {
        // default constructor, empty
    }

    public PrepareMessage(long executionId) {
        super(executionId);
    }

    public PrepareMessage(long executionId, long proposalNumber) {
        this(executionId);
        this.proposalNumber = proposalNumber;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.Prepare;
    }

    @Override
    public String toString() {
        return String.format("<Prepare src='%s' executionId='%d', N='%d' />", getSourceAddress(), getExecutionId(), getProposalNumber());
    }
}
