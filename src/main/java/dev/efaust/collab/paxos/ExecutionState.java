package dev.efaust.collab.paxos;

import lombok.Getter;
import lombok.Setter;

public class ExecutionState {
    // prepare: The number n must be greater than any number used in any of the previous Prepare messages by this Proposer.
    @Getter @Setter
    private long priorPrepareN = 0;

    @Getter @Setter
    public long acceptorPromiseN = 0;

    public ExecutionState() {
        this.priorPrepareN = 0;
    }
}
