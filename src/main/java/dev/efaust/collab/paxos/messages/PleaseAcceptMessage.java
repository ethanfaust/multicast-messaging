package dev.efaust.collab.paxos.messages;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode(callSuper = true)
public class PleaseAcceptMessage extends PaxosMessage {
    @Getter @Setter
    private long proposalNumberToAccept;

    @Getter @Setter
    private long valueToAccept;

    @Override
    public MessageType getMessageType() {
        return MessageType.PleaseAccept;
    }

    @Override
    public String toString() {
        return String.format("<PleaseAccept src='%s' executionId='%d', N='%d' V='%d' />",
                getSourceAddress(), getExecutionId(), getProposalNumberToAccept(), getValueToAccept());
    }
}
