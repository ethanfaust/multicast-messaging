package dev.efaust.collab.paxos;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.Getter;
import lombok.Setter;

public class PrepareMessage extends PaxosMessage {
    // names according to convention here: https://en.wikipedia.org/wiki/Paxos_%28computer_science%29#Basic_Paxos
    @Getter @Setter
    private long n;

    public PrepareMessage() {
        // default constructor, empty
    }

    public PrepareMessage(long executionId, long n) {
        super(executionId);
        this.n = n;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.Prepare;
    }

    @Override
    public String toString() {
        return String.format("<Prepare src='%s' executionId='%d', n='%d' />", getSourceAddress(), getExecutionId(), getN());
    }
}
