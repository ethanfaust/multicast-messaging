package dev.efaust.collab.messaging;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Enable simulation of a network of nodes. Each node has a send queue and a receive queue.
 * This interconnect facilitates exchange of messages by taking messages from each node's send queue
 * and propagating to all other receive queues.
 */
public class InMemoryInterconnect {
    private static Logger log = LogManager.getLogger(InMemoryInterconnect.class);

    private List<InMemoryMessagingLayer> nodes;

    @Getter
    private List<MessageHistoryEntry> history;

    public InMemoryInterconnect() {
        this.nodes = new ArrayList<>();
        this.history = new ArrayList<>();
    }

    public void addNode(InMemoryMessagingLayer node) {
        this.nodes.add(node);
    }

    private Message setMessageSrcAddress(Message message, String srcNodeId) throws IOException {
        Message sentMessage = null;
        try {
            sentMessage = (Message)message.clone();
        } catch (CloneNotSupportedException e) {
            throw new IOException(e);
        }
        sentMessage.setSourceAddress(srcNodeId);
        return sentMessage;
    }

    // TODO: add dropped messages, random reordering, etc.
    public void exchangeMessagesOnce() throws IOException {
        for (InMemoryMessagingLayer send : nodes) {
            Message message = send.getSendQueue().poll();
            if (message == null) {
                continue;
            }
            Message sentMessage = setMessageSrcAddress(message, send.getNodeId());

            for (InMemoryMessagingLayer receive : nodes) {
                receive.getReceiveQueue().add(sentMessage);

                // Record history entry with src, dst, message
                MessageHistoryEntry historyEntry = new MessageHistoryEntry();
                historyEntry.setSrcNode(send.getNodeId());
                historyEntry.setDstNode(receive.getNodeId());
                historyEntry.setMessage(sentMessage);
                history.add(historyEntry);
            }
        }
    }

    // Assumes queues are not repopulated after they are emptied.
    // This is intended to support unit tests, which are generally single-threaded, so this assumption will hold.
    public void drainQueues() throws IOException {
        Map<String, Boolean> queuesDrained = new HashMap<>();
        while (true) {
            exchangeMessagesOnce();
            updateQueueDrainStatus(queuesDrained);
            log.debug("queues drained: {}", queuesDrained);
            boolean allQueuesDrained = !queuesDrained.containsValue(false);
            log.debug("allQueuesDrained: {}", allQueuesDrained);
            if (allQueuesDrained) {
                break;
            }
        }
    }

    private void updateQueueDrainStatus(Map<String, Boolean> queuesDrained) {
        for (InMemoryMessagingLayer node : nodes) {
            boolean queueDrainedForNode = node.getSendQueue().peek() == null;
            queuesDrained.put(node.getNodeId(), queueDrainedForNode);
        }
    }
}
