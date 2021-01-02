package dev.efaust.collab;

import lombok.Getter;

import java.util.Optional;

public enum MessageType {
    Heartbeat(0),
    Prepare(1),
    Promise(2),
    PleaseAccept(3),
    Accepted(4),
    NegativePromise(5);

    @Getter
    byte id;

    MessageType(int id) {
        this.id = (byte)id;
    }

    public static Optional<MessageType> messageTypeFromId(byte id) {
        for (MessageType type : MessageType.values()) {
            if (type.id == id) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }
}
