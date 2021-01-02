package dev.efaust.collab.paxos;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.Getter;
import lombok.Setter;

public class AcceptedMessage extends PaxosMessage {
    @Getter @Setter
    private long acceptedN;

    @Getter @Setter
    private long acceptedValue;

    @Override
    public MessageType getMessageType() {
        return MessageType.Accepted;
    }

    @Override
    public String toString() {
        return String.format("<Accepted src='%s' executionId='%d', acceptedN='%d' acceptedValue='%d' />",
                getSourceAddress(), getExecutionId(), getAcceptedN(), getAcceptedValue());
    }
}
