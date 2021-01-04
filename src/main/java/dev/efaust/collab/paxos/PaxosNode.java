package dev.efaust.collab.paxos;

import com.google.common.collect.Sets;
import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.liveness.PeerRegistry;
import dev.efaust.collab.messaging.Message;
import dev.efaust.collab.messaging.MessagingLayer;
import dev.efaust.collab.paxos.messages.*;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

/**
 * Implements one Paxos "Processor" (e.g. one machine on a network seeking consensus with other machines on
 * a series of transactions).
 * Paxos ref: https://en.wikipedia.org/wiki/Paxos_%28computer_science%29#Basic_Paxos
 */
public class PaxosNode {
    private static final Logger log = LogManager.getLogger(PaxosNode.class);

    @Getter
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

    public void receiveMessages() {
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
            AcceptedMessage accepted = AcceptedMessage.class.cast(message);
            receiveAccepted(accepted);
        } else {
            log.warn("no handler for message type, message {}", message);
        }
    }

    private void receiveAccepted(AcceptedMessage accepted) {
        // record Accepted
        long executionId = accepted.getExecutionId();
        ExecutionState state = ensureExecutionStateExists(executionId);
        state.getAcceptedMessages().add(accepted);
    }

    private void receivePrepare(PrepareMessage prepare) throws IOException {
        long executionId = prepare.getExecutionId();
        ExecutionState state = ensureExecutionStateExists(executionId);
        long messageN = prepare.getProposalNumber();
        long priorN = state.getPriorPrepareN();
        log.info("[{}] received prepare, message N: {}, prior N: {}", nodeId, messageN, priorN);

        Optional<Long> priorAcceptedProposalNumber = state.getAccepts().keySet().stream().max(Long::compare);
        Optional<Long> priorAcceptedProposalValue = Optional.empty();
        if (priorAcceptedProposalNumber.isPresent()) {
            priorAcceptedProposalValue = Optional.of(state.getAccepts().get(priorAcceptedProposalNumber.get()));
        }

        if (messageN > priorN) {
            log.info("[{}] message N is greater, making promise", nodeId);

            // return promise
            state.setPriorPrepareN(messageN);

            PromiseMessage promise = new PromiseMessage();
            promise.setExecutionId(executionId);
            promise.setPromiseProposalNumber(messageN);
            promise.setPriorAcceptedProposalNumber(priorAcceptedProposalNumber.orElse(PromiseMessage.NO_PRIOR_ACCEPTED_N));
            promise.setPriorAcceptedValue(priorAcceptedProposalValue.orElse(PromiseMessage.NO_PRIOR_ACCEPTED_VALUE));
            sendMessage(promise);
        } else {
            log.info("[{}] message N is not greater, not making promise", nodeId);

            // return negative promise
            NegativePromiseMessage negativePromise = new NegativePromiseMessage();
            negativePromise.setExecutionId(executionId);
            negativePromise.setProposalNumber(messageN);
            negativePromise.setPriorPromisedProposalNumber(priorN);
            negativePromise.setPriorAcceptedProposalNumber(priorAcceptedProposalNumber.orElse(PromiseMessage.NO_PRIOR_ACCEPTED_N));
            negativePromise.setPriorAcceptedValue(priorAcceptedProposalValue.orElse(PromiseMessage.NO_PRIOR_ACCEPTED_VALUE));

            // TODO: enable in future
            //sendMessage(negativePromise);
        }
    }

    private void receivePromise(PromiseMessage promise) throws IOException {
        long executionId = promise.getExecutionId();
        ExecutionState state = ensureExecutionStateExists(executionId);

        // merge this message into current local state
        state.getPromises().put(promise.getSourceAddress(), promise);

        // need a majority of promises from a quorum of acceptors to proceed
        // TODO: does this need to be for N?
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
                .max(Comparator.comparingLong(PromiseMessage::getPriorAcceptedProposalNumber));

        long valueToAccept;
        if (maxPriorAcceptedOptional.isPresent()) {
            PromiseMessage maxPriorAccepted = maxPriorAcceptedOptional.get();
            log.info("[{}] max prior accepted n={} v={} src={}", nodeId, maxPriorAccepted.getPriorAcceptedProposalNumber(),
                    maxPriorAccepted.getPriorAcceptedValue(), maxPriorAccepted.getSourceAddress());
            valueToAccept = maxPriorAccepted.getPriorAcceptedValue();
        } else {
            log.info("[{}] no max prior accepted, we get to pick the value", nodeId);
            Optional<Long> valueToAcceptOptional = pickProposedValueToAccept(state);
            if (!valueToAcceptOptional.isPresent()) {
                // This node doesn't want to provide a value for this execution.
                // That's ok, it can do nothing.
                // Return, don't send any PleaseAccept message.
                log.info("[{}] we do not want to pick a value, doing nothing", nodeId);
                return;
            }
            valueToAccept = valueToAcceptOptional.get();
        }

        PleaseAcceptMessage pleaseAcceptMessage = new PleaseAcceptMessage();
        pleaseAcceptMessage.setExecutionId(executionId);
        pleaseAcceptMessage.setProposalNumberToAccept(promise.getPromiseProposalNumber());
        pleaseAcceptMessage.setValueToAccept(valueToAccept);
        // reduce duplicate messages
        if (!state.getPriorSentPleaseAccept().contains(pleaseAcceptMessage)) {
            sendMessage(pleaseAcceptMessage);
        } else {
            log.info("[{}] not sending duplicate PleaseAccept", nodeId);
        }
        state.getPriorSentPleaseAccept().add(pleaseAcceptMessage);
    }

    private Optional<Long> pickProposedValueToAccept(ExecutionState state) {
        if (state.getDesiredValueOptional().isPresent()) {
            return state.getDesiredValueOptional();
        }
        if (!state.getDesiredValueSupplierOptional().isPresent()) {
            return Optional.empty();
        }
        long desiredValue = state.getDesiredValueSupplierOptional().get().get();
        Optional<Long> desiredValueOptional = Optional.of(desiredValue);
        state.setDesiredValueOptional(desiredValueOptional);
        return desiredValueOptional;
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

    private void logPriorAccepts(ExecutionState state) {
        state.getAccepts().keySet().stream().sorted(Long::compare).forEachOrdered((acceptProposalNumber) -> {
            long acceptValue = state.getAccepts().get(acceptProposalNumber);
            log.info("  priorAccept N={} V={}", acceptProposalNumber, acceptValue);
        });
    }

    private void receiveAccept(PleaseAcceptMessage accept) throws IOException {
        long executionId = accept.getExecutionId();
        ExecutionState state = ensureExecutionStateExists(executionId);

        // accept if and only if we have not promised not to
        long proposalNumber = accept.getProposalNumberToAccept();

        log.info("[{}] receiveAccept proposal number {}", nodeId, proposalNumber);
        logPriorAccepts(state);

        // only ever accept a single value per proposal number
        if (!state.getAccepts().containsKey(proposalNumber)) {
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
            log.info("[{}] ACCEPT, NO CONFLICT {}", nodeId, accept);
            for (PromiseMessage promiseMessage : state.getPromises().values()) {
                log.info("  [{}] prior promise {}", nodeId, promiseMessage);
            }
            state.getAccepts().put(proposalNumber, accept.getValueToAccept());

            AcceptedMessage acceptedMessage = new AcceptedMessage();
            acceptedMessage.setExecutionId(executionId);
            acceptedMessage.setAcceptedProposalNumber(proposalNumber);
            acceptedMessage.setAcceptedValue(state.getAccepts().get(proposalNumber));
            sendMessage(acceptedMessage);
        }
    }

    public long sendPrepare(Supplier<Long> desiredValueSupplier) throws IOException {
        long executionId = getNextExecutionId();
        sendPrepare(executionId, desiredValueSupplier);
        return executionId;
    }

    public void sendPrepare(long executionId, Supplier<Long> desiredValueSupplier) throws IOException {
        ExecutionState state = ensureExecutionStateExists(executionId);
        long prepareN = state.getPriorPrepareN() + 1;
        state.setDesiredValueSupplierOptional(Optional.of(desiredValueSupplier));

        // this is done on receiving our own message
        //state.setPriorPrepareN(prepareN);
        // TODO: what if we don't receive our own message?
        // solution 1: if src=us and dst=us, bypass message channel and execute state machine directly?
        // solution 2: model prepare/promise state separately, ignore NegativePromise from self

        sendMessage(new PrepareMessage(executionId, prepareN));
    }

    public void sendMessage(Message message) throws IOException {
        messagingLayer.send(message);
        log.info("[{}] sent {}", nodeId, message);
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
