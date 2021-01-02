package dev.efaust.collab.paxos;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.paxos.PaxosMessage;
import lombok.Getter;
import lombok.Setter;

// Negative accept response, indicating that the node has already promised not to accept this value of N
public class NegativePromiseMessage extends PaxosMessage {
    // names according to convention here: https://en.wikipedia.org/wiki/Paxos_%28computer_science%29#Basic_Paxos
    @Getter @Setter
    private long n;

    @Getter @Setter
    private long priorPromisedN;

    @Getter @Setter
    private long priorAcceptedN;

    @Getter @Setter
    private long priorAcceptedValue;

    @Override
    public MessageType getMessageType() {
        return MessageType.NegativePromise;
    }

    @Override
    public String toString() {
        return String.format("<NegativePromise src='%s' executionId='%d', n='%d' priorPromisedN='%d' priorAcceptedN='%d' priorAcceptedValue='%d' />",
                getSourceAddress(), getExecutionId(), getN(), getPriorPromisedN(), getPriorAcceptedN(), getPriorAcceptedValue());
    }
}
