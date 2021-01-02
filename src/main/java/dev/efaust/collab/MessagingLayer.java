package dev.efaust.collab;

import dev.efaust.collab.Message;

import java.io.IOException;
import java.util.Queue;

public interface MessagingLayer {
    void send(Message message) throws IOException;
    Queue<Message> getReceiveQueue();
}
