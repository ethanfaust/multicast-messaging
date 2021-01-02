package dev.efaust.collab.paxos;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.Getter;
import lombok.Setter;

public class PleaseAcceptMessage extends PaxosMessage {
    @Getter @Setter
    private long pleaseAcceptN;

    @Getter @Setter
    private long valueToAccept;

    @Override
    public MessageType getMessageType() {
        return MessageType.PleaseAccept;
    }

    @Override
    public String toString() {
        return String.format("<PleaseAccept src='%s' executionId='%d', pleaseAcceptN='%d' valueToAccept='%d' />",
                getSourceAddress(), getExecutionId(), getPleaseAcceptN(), getValueToAccept());
    }
}
