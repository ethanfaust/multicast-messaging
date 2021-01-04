package dev.efaust.collab.messaging;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.paxos.PaxosMessage;
import dev.efaust.collab.paxos.messages.AcceptedMessage;
import dev.efaust.collab.paxos.messages.PleaseAcceptMessage;
import dev.efaust.collab.paxos.messages.PrepareMessage;
import dev.efaust.collab.paxos.messages.PromiseMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
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

        ByteBuffer byteBuffer = ByteBuffer.allocate(256);
        byteBuffer.put(MAGIC);
        byteBuffer.put(VERSION);
        byteBuffer.put(messageType.getId());

        if (message instanceof PaxosMessage) {
            PaxosMessage paxosMessage = PaxosMessage.class.cast(message);
            byteBuffer.putLong(paxosMessage.getExecutionId());

            if (message instanceof PrepareMessage) {
                PrepareMessage prepareMessage = PrepareMessage.class.cast(message);
                byteBuffer.putLong(prepareMessage.getProposalNumber());
            } else if (message instanceof PromiseMessage) {
                PromiseMessage promiseMessage = PromiseMessage.class.cast(message);
                byteBuffer.putLong(promiseMessage.getPromiseProposalNumber());
                byteBuffer.putLong(promiseMessage.getPriorAcceptedProposalNumber());
                byteBuffer.putLong(promiseMessage.getPriorAcceptedValue());
            } else if (message instanceof PleaseAcceptMessage) {
                PleaseAcceptMessage pleaseAcceptMessage = PleaseAcceptMessage.class.cast(message);
                byteBuffer.putLong(pleaseAcceptMessage.getProposalNumberToAccept());
                byteBuffer.putLong(pleaseAcceptMessage.getValueToAccept());
            } else if (message instanceof AcceptedMessage) {
                AcceptedMessage acceptedMessage = AcceptedMessage.class.cast(message);
                byteBuffer.putLong(acceptedMessage.getAcceptedProposalNumber());
                byteBuffer.putLong(acceptedMessage.getAcceptedValue());
            } else {
                throw new RuntimeException(String.format("serialize called for unknown paxos message type, message %s", message));
            }
            // TODO: serialization support for NegativePromise and other message types
        } else if (message instanceof HeartbeatMessage) {
            HeartbeatMessage heartbeatMessage = HeartbeatMessage.class.cast(message);
            byteBuffer.putLong(heartbeatMessage.getUuid());
        } else {
            throw new RuntimeException(String.format("serialize called for unknown message type, message %s", message));
        }
        return Arrays.copyOf(byteBuffer.array(), byteBuffer.position());
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
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        try {
            byteBuffer.position(MAGIC.length);
            byte version = byteBuffer.get();
            byte messageTypeByte = byteBuffer.get();

            Optional<MessageType> messageType = MessageType.messageTypeFromId(messageTypeByte);
            if (!messageType.isPresent()) {
                throw new IOException(String.format("could not determine message type for value %d", messageTypeByte));
            }
            switch (messageType.get()) {
                case Heartbeat:
                    HeartbeatMessage heartbeatMessage = new HeartbeatMessage();
                    heartbeatMessage.setUuid(byteBuffer.getLong());
                    message = Optional.of(heartbeatMessage);
                    break;
                case Prepare:
                    PrepareMessage prepareMessage = new PrepareMessage();
                    prepareMessage.setExecutionId(byteBuffer.getLong());
                    prepareMessage.setProposalNumber(byteBuffer.getLong());
                    message = Optional.of(prepareMessage);
                    break;
                case Promise:
                    PromiseMessage promiseMessage = new PromiseMessage();
                    promiseMessage.setExecutionId(byteBuffer.getLong());
                    promiseMessage.setPromiseProposalNumber(byteBuffer.getLong());
                    promiseMessage.setPriorAcceptedProposalNumber(byteBuffer.getLong());
                    promiseMessage.setPriorAcceptedValue(byteBuffer.getLong());
                    message = Optional.of(promiseMessage);
                    break;
                case PleaseAccept:
                    PleaseAcceptMessage pleaseAcceptMessage = new PleaseAcceptMessage();
                    pleaseAcceptMessage.setExecutionId(byteBuffer.getLong());
                    pleaseAcceptMessage.setProposalNumberToAccept(byteBuffer.getLong());
                    pleaseAcceptMessage.setValueToAccept(byteBuffer.getLong());
                    message = Optional.of(pleaseAcceptMessage);
                    break;
                case Accepted:
                    AcceptedMessage acceptedMessage = new AcceptedMessage();
                    acceptedMessage.setExecutionId(byteBuffer.getLong());
                    acceptedMessage.setAcceptedProposalNumber(byteBuffer.getLong());
                    acceptedMessage.setAcceptedValue(byteBuffer.getLong());
                    message = Optional.of(acceptedMessage);
                    break;
                // TODO: add support for deserializing other message types, e.g. NegativePromiseMessage
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
