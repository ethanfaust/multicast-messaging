package dev.efaust.collab.paxos;

import dev.efaust.collab.MessageType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode
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
