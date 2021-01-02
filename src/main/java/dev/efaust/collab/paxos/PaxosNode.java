package dev.efaust.collab.paxos;

import com.google.common.collect.Sets;
import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.liveness.PeerRegistry;
import dev.efaust.collab.messaging.Message;
import dev.efaust.collab.messaging.MessagingLayer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.*;

/**
 * Implements one Paxos "Processor" (e.g. one machine on a network seeking consensus with other machines on
 * a series of transactions).
 * Paxos ref: https://en.wikipedia.org/wiki/Paxos_%28computer_science%29#Basic_Paxos
 */
public class PaxosNode {
    private static Logger log = LogManager.getLogger(PaxosNode.class);

    private String nodeId;
    private MessagingLayer messagingLayer;
    private PeerRegistry peerRegistry;
    int executionId;

    private Map<Long, ExecutionState> executionStates;

    public PaxosNode(String nodeId, MessagingLayer messagingLayer) {
        this.nodeId = nodeId;
        this.messagingLayer = messagingLayer;
        this.executionId = 0;
        this.peerRegistry = new PeerRegistry();
        this.executionStates = new HashMap<>();
    }

    public long getNextExecutionId() {
        return executionStates.keySet().stream().max(Long::compare).orElse(0L) + 1;
    }

    public void receiveMessages() throws IOException {
        while (true) {
            Message message = messagingLayer.getReceiveQueue().poll();
            if (message == null) {
                break;
            }
            try {
                receiveMessage(message);
            } catch (IOException e) {
                log.error("error receiving message", e);
            }
        }
    }

    public void receiveMessage(Message message) throws IOException {
        log.info("[{}] received {}", nodeId, message);
        if (message instanceof HeartbeatMessage) {
            peerRegistry.updatePeerHeartbeat(message.getSourceAddress(), DateTime.now(DateTimeZone.UTC));
        } else if (message instanceof PrepareMessage) {
            PrepareMessage prepare = PrepareMessage.class.cast(message);
            receivePrepare(prepare);
        } else if (message instanceof PromiseMessage) {
            PromiseMessage promise = PromiseMessage.class.cast(message);
            receivePromise(promise);
        } else if (message instanceof NegativePromiseMessage) {
            NegativePromiseMessage negativePromise = NegativePromiseMessage.class.cast(message);
            // TODO: handle
        } else if (message instanceof PleaseAcceptMessage) {
            PleaseAcceptMessage accept = PleaseAcceptMessage.class.cast(message);
            receiveAccept(accept);
        } else if (message instanceof AcceptedMessage) {
            // TODO: handle
        }
    }

    private void receivePrepare(PrepareMessage prepare) throws IOException {
        long executionId = prepare.getExecutionId();
        ExecutionState state = ensureExecutionStateExists(executionId);
        long messageN = prepare.getProposalNumber();
        long priorN = state.getPriorPrepareN();
        if (messageN > priorN) {
            // return promise
            state.setPriorPrepareN(messageN);

            PromiseMessage promise = new PromiseMessage();
            promise.setExecutionId(executionId);
            promise.setPromiseProposalNumber(messageN);
            promise.setPriorAcceptedProposalNumber(state.getAcceptedN().orElse(PromiseMessage.NO_PRIOR_ACCEPTED_N));
            promise.setPriorAcceptedValue(state.getAcceptedValue().orElse(PromiseMessage.NO_PRIOR_ACCEPTED_VALUE));
            sendMessage(promise);
        } else {
            // return negative promise
            NegativePromiseMessage negativePromise = new NegativePromiseMessage();
            negativePromise.setExecutionId(executionId);
            negativePromise.setProposalNumber(messageN);
            negativePromise.setPriorPromisedProposalNumber(priorN);
            negativePromise.setPriorAcceptedProposalNumber(state.getAcceptedN().orElse(PromiseMessage.NO_PRIOR_ACCEPTED_N));
            negativePromise.setPriorAcceptedValue(state.getAcceptedValue().orElse(PromiseMessage.NO_PRIOR_ACCEPTED_VALUE));
            sendMessage(negativePromise);
        }
    }

    private void receivePromise(PromiseMessage promise) throws IOException {
        long executionId = promise.getExecutionId();
        ExecutionState state = ensureExecutionStateExists(executionId);

        // merge this message into current local state
        state.getPromises().put(promise.getSourceAddress(), promise);

        // need a majority of promises from a quorum of acceptors to proceed
        if (!haveMajorityOfPromises(state)) {
            log.info("[{}] do not yet have a majority of promises, cannot proceed with accept", nodeId);
            return;
        }

        log.info("[{}] obtained a majority of promises, we can proceed!", nodeId);

        for (PromiseMessage otherPromise : state.getPromises().values()) {
            log.info("  prior promise {}", otherPromise);
        }
        Optional<PromiseMessage> maxPriorAcceptedOptional = state.getPromises().values().stream()
                .filter((p) -> p.getPriorAcceptedProposalNumber() != PromiseMessage.NO_PRIOR_ACCEPTED_N)
                .max(Comparator.comparingLong((p) -> p.getPriorAcceptedProposalNumber()));

        long valueToAccept;
        if (maxPriorAcceptedOptional.isPresent()) {
            PromiseMessage maxPriorAccepted = maxPriorAcceptedOptional.get();
            log.info("[{}] max prior accepted n={} v={} src={}", nodeId, maxPriorAccepted.getPriorAcceptedProposalNumber(),
                    maxPriorAccepted.getPriorAcceptedValue(), maxPriorAccepted.getSourceAddress());
            valueToAccept = maxPriorAccepted.getPriorAcceptedValue();
        } else {
            log.info("[{}] no max prior accepted, we get to pick the value", nodeId);
            // TODO: make this something useful
            valueToAccept = 42L;
        }

        PleaseAcceptMessage pleaseAcceptMessage = new PleaseAcceptMessage();
        pleaseAcceptMessage.setExecutionId(executionId);
        pleaseAcceptMessage.setProposalNumberToAccept(promise.getPromiseProposalNumber());
        pleaseAcceptMessage.setValueToAccept(valueToAccept);
        sendMessage(pleaseAcceptMessage);
    }

    private boolean haveMajorityOfPromises(ExecutionState state) {
        Set<String> peersAlive = peerRegistry.peersAlive();
        log.info("[{}] peers alive: {}", nodeId, peersAlive);
        Set<String> promisesReceived = state.getPromises().keySet();
        log.info("[{}] promises received: {}", nodeId, promisesReceived);
        Set<String> promisesNotReceived = Sets.difference(peersAlive, promisesReceived);
        log.info("[{}] promises not received: {}", nodeId, promisesNotReceived);
        log.info("[{}] peers alive: {}, promises received: {}, promises not received: {}", nodeId,
                peersAlive.size(), promisesReceived.size(), promisesNotReceived.size());
        return promisesReceived.size() > promisesNotReceived.size();
    }

    private void receiveAccept(PleaseAcceptMessage accept) throws IOException {
        long executionId = accept.getExecutionId();
        ExecutionState state = ensureExecutionStateExists(executionId);

        // accept if and only if we have not promised not to
        long proposalNumber = accept.getProposalNumberToAccept();

        // Check promises, make sure there are no conflicts.
        // Each promise indicates that we should ignore all future proposals with number less than N.
        // Find conflicts: number > N. Equal N can pass.
        Optional<PromiseMessage> conflictOptional = state.getPromises().values().stream()
                .filter((promise) -> promise.getPromiseProposalNumber() > proposalNumber)
                .findAny();
        if (conflictOptional.isPresent()) {
            log.info("[{}] got PleaseAccept n={}, CONFLICT {}, NOT ACCEPTING", nodeId, proposalNumber, conflictOptional.get());
            return;
        }
        log.info("[{}] ACCEPT {}", nodeId, accept);
        state.setAcceptedN(Optional.of(proposalNumber));
        state.setAcceptedValue(Optional.of(accept.getValueToAccept()));

        AcceptedMessage acceptedMessage = new AcceptedMessage();
        acceptedMessage.setExecutionId(executionId);
        acceptedMessage.setAcceptedProposalNumber(proposalNumber);
        acceptedMessage.setAcceptedValue(accept.getValueToAccept());
        sendMessage(acceptedMessage);
    }

    public long sendPrepare() throws IOException {
        long executionId = getNextExecutionId();
        sendPrepare(executionId);
        return executionId;
    }

    public void sendPrepare(long executionId) throws IOException {
        ExecutionState state = ensureExecutionStateExists(executionId);
        long prepareN = state.getPriorPrepareN() + 1;
        state.setPriorPrepareN(prepareN);
        sendMessage(new PrepareMessage(executionId, prepareN));
    }

    public void sendMessage(Message message) throws IOException {
        messagingLayer.send(message);
    }

    public PeerRegistry getPeerRegistry() {
        return peerRegistry;
    }

    public ExecutionState getExecutionState(long executionId) {
        return executionStates.get(executionId);
    }

    public ExecutionState ensureExecutionStateExists(long executionId) {
        executionStates.putIfAbsent(executionId, new ExecutionState());
        ExecutionState state = executionStates.get(executionId);
        return state;
    }
}
