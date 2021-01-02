package dev.efaust.collab.messaging;

import dev.efaust.collab.MessageType;
import lombok.Getter;
import lombok.Setter;

/**
 * Base class for all messages.
 * This is the Java representation, these might be serialized and put on the wire as UDP packets.
 */
public abstract class Message implements Cloneable {
    @Getter @Setter
    private String sourceAddress;

    public abstract MessageType getMessageType();

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}