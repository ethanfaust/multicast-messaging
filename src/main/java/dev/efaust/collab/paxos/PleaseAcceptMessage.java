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
}
