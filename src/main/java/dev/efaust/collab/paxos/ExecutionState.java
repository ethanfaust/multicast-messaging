package dev.efaust.collab.paxos;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionState {
    // prepare: The number n must be greater than any number used in any of the previous Prepare messages by this Proposer.
    @Getter @Setter
    private long priorPrepareN = 0;

    @Getter @Setter
    private long acceptorPromiseN = 0;

    @Getter
    private Map<String, PromiseMessage> promises;

    @Getter @Setter
    private Optional<Long> acceptedN = Optional.empty();

    @Getter @Setter
    private Optional<Long> acceptedValue = Optional.empty();

    public ExecutionState() {
        this.priorPrepareN = 0;
        this.promises = new HashMap<>();
    }
}
