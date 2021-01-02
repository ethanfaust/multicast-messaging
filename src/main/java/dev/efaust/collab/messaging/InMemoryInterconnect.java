package dev.efaust.collab.messaging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryInterconnect {
    private static Logger log = LogManager.getLogger(InMemoryInterconnect.class);

    private List<InMemoryMessagingLayer> nodes;

    public InMemoryInterconnect() {
        this.nodes = new ArrayList<>();
    }

    public void addNode(InMemoryMessagingLayer node) {
        this.nodes.add(node);
    }

    public void exchangeMessagesOnce() throws IOException {
        for (InMemoryMessagingLayer send : nodes) {
            Message message = send.getSendQueue().poll();
            if (message == null) {
                continue;
            }
            for (InMemoryMessagingLayer receive : nodes) {
                Message receivedMessage = null;
                try {
                    receivedMessage = (Message)message.clone();
                } catch (CloneNotSupportedException e) {
                    throw new IOException(e);
                }
                receivedMessage.sourceAddress = send.getNodeId();
                receive.getReceiveQueue().add(receivedMessage);
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
            log.info("queues drained: {}", queuesDrained);
            boolean allQueuesDrained = !queuesDrained.containsValue(false);
            log.info("allQueuesDrained: {}", allQueuesDrained);
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
