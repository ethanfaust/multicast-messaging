package dev.efaust.collab.paxos;

import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.liveness.PeerRegistry;
import dev.efaust.collab.messaging.Message;
import dev.efaust.collab.messaging.MessagingLayer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
        log.info("received {}", message);
        if (message instanceof HeartbeatMessage) {
            peerRegistry.updatePeerHeartbeat(message.sourceAddress, DateTime.now(DateTimeZone.UTC));
        } else if (message instanceof PrepareMessage) {
            PrepareMessage prepare = PrepareMessage.class.cast(message);
            receivePrepare(prepare);
        } else if (message instanceof PromiseMessage) {
            PromiseMessage promise = PromiseMessage.class.cast(message);
            // need a quorum of promises to proceed

        } else if (message instanceof NegativePromiseMessage) {
            NegativePromiseMessage negativePromise = NegativePromiseMessage.class.cast(message);
        }
    }

    private void receivePrepare(PrepareMessage prepare) throws IOException {
        long executionId = prepare.getExecutionId();
        executionStates.put(executionId, new ExecutionState());
        ExecutionState state = executionStates.get(executionId);
        long messageN = prepare.getN();
        long priorN = state.getPriorPrepareN();
        if (messageN > priorN) {
            // return promise
            state.setPriorPrepareN(messageN);

            // TODO: fix -1 values
            PromiseMessage promise = new PromiseMessage();
            promise.setExecutionId(executionId);
            promise.setPromiseN(messageN);
            promise.setPriorAcceptedN(-1);
            promise.setPriorAcceptedValue(-1);
            sendMessage(promise);
        } else {
            // return negative promise
            NegativePromiseMessage negativePromise = new NegativePromiseMessage();
            negativePromise.setExecutionId(executionId);
            negativePromise.setN(messageN);
            negativePromise.setPriorPromisedN(priorN);
            negativePromise.setPriorAcceptedN(-1);
            negativePromise.setPriorAcceptedValue(-1);
            sendMessage(negativePromise);
        }
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
}
