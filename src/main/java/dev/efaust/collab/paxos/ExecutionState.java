package dev.efaust.collab.paxos;

import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.function.Supplier;

public class ExecutionState {
    // prepare: The number n must be greater than any number used in any of the previous Prepare messages by this Proposer.
    @Getter @Setter
    private long priorPrepareN = 0;

    // nodeId -> promise
    @Getter
    private Map<String, PromiseMessage> promises;

    @Getter
    private Set<PleaseAcceptMessage> priorSentPleaseAccept;

    // proposalNumber -> value
    @Getter
    private Map<Long, Long> accepts;

    // Supplier to provide a value that this node would like selected for this execution.
    // Optional because the node might not care (e.g. unit test where A and B are proposing values but C is not).
    // Supplier is used exactly once.
    @Getter @Setter
    private Optional<Supplier<Long>> desiredValueSupplierOptional = Optional.empty();

    // Value that this node would like selected (if any)
    @Getter @Setter
    private Optional<Long> desiredValueOptional = Optional.empty();

    public ExecutionState() {
        this.priorPrepareN = 0;
        this.promises = new HashMap<>();
        this.priorSentPleaseAccept = new HashSet<>();
        this.accepts = new HashMap<>();
    }
}
