package dev.efaust.collab.messaging;

import dev.efaust.collab.messaging.Message;

import java.io.IOException;
import java.util.Queue;

/**
 * High level interface of messaging.
 * Used to decouple messaging logic from wire implementation.
 * (e.g. here there is an in memory implementation and a UDP multicast implementation)
 */
public interface MessagingLayer {
    void send(Message message) throws IOException;
    Queue<Message> getReceiveQueue();
}
