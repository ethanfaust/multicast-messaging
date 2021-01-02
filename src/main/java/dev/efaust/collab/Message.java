package dev.efaust.collab;

import dev.efaust.collab.MessageType;

public abstract class Message {
    public byte[] bytes;
    public String sourceAddress;

    public abstract MessageType getMessageType();
}