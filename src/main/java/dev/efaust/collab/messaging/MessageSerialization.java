package dev.efaust.collab.messaging;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.liveness.HeartbeatMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Convert in memory Java objects to byte representation to be sent in UDP packets.
 */
public class MessageSerialization {
    /**
     *  The current implementation is very basic.
     *  This is probably better done with something like Protocol Buffers.
     */

    Logger log = LogManager.getLogger(MessageSerialization.class);

    // wire format:
    // magic: 5 bytes
    // version: 1 byte
    // message type: 1 byte
    // ... (varies depending on message type)

    static byte[] MAGIC = new byte[]{ 0xc, 0x0, 0x1, 0x1, 0xa, 0xb };
    static byte VERSION = 0;

    private int getHeaderLength() {
        // magic + version + message type
        return MAGIC.length + 2;
    }

    public byte[] serialize(Message message) {
        int length = getHeaderLength();
        MessageType messageType = message.getMessageType();
        // depending on message type, length might vary

        ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        byteBuffer.put(MAGIC);
        byteBuffer.put(VERSION);
        byteBuffer.put(messageType.getId());

        // TODO: serialization support for other message types

        return byteBuffer.array();
    }

    public boolean validate(byte[] bytes) {
        if (!hasExpectedHeaderLength(bytes) || !hasValidMagic(bytes)) {
            return false;
        }
        return true;
    }

    public Optional<Message> deserialize(byte[] bytes) {
        if (!validate(bytes)) {
            return Optional.empty();
        }
        Optional<Message> message = Optional.empty();
        try {
            MessageType messageType = getMessageType(bytes);
            switch (messageType) {
                case Heartbeat:
                    message = Optional.of(new HeartbeatMessage());
                    break;
                // TODO: add support for deserializing other message types
            }
        } catch (IOException e) {
            log.warn("failed to deserialize message", e);
        }
        return message;
    }

    private boolean hasExpectedHeaderLength(byte[] bytes) {
        return bytes.length >= getHeaderLength();
    }

    private boolean hasValidMagic(byte[] bytes) {
        if (bytes.length < MAGIC.length) {
            return false;
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (bytes[i] != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    private byte getVersion(byte[] bytes) throws IOException {
        if (!hasExpectedHeaderLength(bytes)) {
            throw new IOException("getVersion failed, unexpected message length");
        }
        return bytes[MAGIC.length];
    }

    private MessageType getMessageType(byte[] bytes) throws IOException {
        if (!hasExpectedHeaderLength(bytes)) {
            throw new IOException("getMessageType failed, unexpected message length");
        }
        byte typeId = bytes[MAGIC.length + 1];
        Optional<MessageType> type = MessageType.messageTypeFromId(typeId);
        if (!type.isPresent()) {
            throw new IOException("getMessageType failed, unknown message type " + typeId);
        }
        return type.get();
    }

    public static String bytesArrayToString(byte[] bytes, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i != 0) {
                sb.append(" ");
            }
            sb.append(bytes[i]);
        }
        return sb.toString();
    }
}
