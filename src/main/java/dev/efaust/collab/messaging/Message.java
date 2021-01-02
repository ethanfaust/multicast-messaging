package dev.efaust.collab.messaging;

import dev.efaust.collab.MessageType;
import lombok.Getter;
import lombok.Setter;

public abstract class Message implements Cloneable {
    public byte[] bytes;

    @Getter @Setter
    public String sourceAddress;

    public abstract MessageType getMessageType();

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}