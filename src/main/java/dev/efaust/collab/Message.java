package dev.efaust.collab;

public abstract class Message {
    public byte[] bytes;
    public String sourceAddress;

    public abstract MessageType getMessageType();
}