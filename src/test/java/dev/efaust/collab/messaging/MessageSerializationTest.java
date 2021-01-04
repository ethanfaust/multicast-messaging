package dev.efaust.collab.messaging;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.messaging.MessageSerialization;
import dev.efaust.collab.paxos.messages.PrepareMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;


public class MessageSerializationTest {
    private MessageSerialization messageSerialization;

    @BeforeEach
    public void before() {
        messageSerialization = new MessageSerialization();
    }

    @Test
    public void testSerializationBasics() {
        HeartbeatMessage heartbeatMessage = new HeartbeatMessage();
        heartbeatMessage.setUuid(42);
        byte[] bytes = messageSerialization.serialize(heartbeatMessage);

        // magic + (version 0 + message type 0) + uuid
        ByteBuffer expected = ByteBuffer.allocate(MessageSerialization.MAGIC.length + 2 + 8);
        expected.put(MessageSerialization.MAGIC);
        expected.put(MessageSerialization.VERSION);
        expected.put(MessageType.Heartbeat.getId());
        expected.putLong(42);

        Assertions.assertArrayEquals(expected.array(), bytes);
    }

    @Test
    public void testPrepareSerialization() {
        PrepareMessage prepareMessage = new PrepareMessage();
        prepareMessage.setExecutionId(1);
        prepareMessage.setProposalNumber(42);
        byte[] bytes = messageSerialization.serialize(prepareMessage);
        Optional<Message> received = messageSerialization.deserialize(bytes);
        Assertions.assertTrue(received.isPresent());
        Assertions.assertTrue(received.get() instanceof PrepareMessage);
        PrepareMessage receivedPrepare = PrepareMessage.class.cast(received.get());
        Assertions.assertEquals(1, receivedPrepare.getExecutionId());
        Assertions.assertEquals(42, receivedPrepare.getProposalNumber());
    }
}
