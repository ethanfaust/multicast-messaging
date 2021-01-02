package dev.efaust.collab.messaging;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Object to facilitate in memory simulation of nodes.
 * Each node has a send queue and receive queue.
 * InMemoryInterconnect can link these together.
 */
public class InMemoryMessagingLayer implements MessagingLayer {
    private String nodeId;
    private Queue<Message> sendQueue;
    private Queue<Message> receiveQueue;

    public InMemoryMessagingLayer(String nodeId) {
        this.nodeId = nodeId;
        this.sendQueue = new LinkedBlockingQueue<>();
        this.receiveQueue = new LinkedBlockingQueue<>();
    }

    public void send(Message message) throws IOException {
        this.sendQueue.add(message);
    }

    public Queue<Message> getReceiveQueue() {
        return receiveQueue;
    }

    public Queue<Message> getSendQueue() {
        return sendQueue;
    }

    public String getNodeId() {
        return nodeId;
    }
}
