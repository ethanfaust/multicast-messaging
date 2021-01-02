package dev.efaust.collab.messaging;

import dev.efaust.collab.messaging.Message;

import java.io.IOException;
import java.util.Queue;

public interface MessagingLayer {
    void send(Message message) throws IOException;
    Queue<Message> getReceiveQueue();
}
