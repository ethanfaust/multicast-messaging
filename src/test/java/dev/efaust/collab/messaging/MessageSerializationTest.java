package dev.efaust.collab.messaging;

import dev.efaust.collab.MessageType;
import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.messaging.MessageSerialization;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class MessageSerializationTest {
    private MessageSerialization messageSerialization;

    @BeforeEach
    public void before() {
        messageSerialization = new MessageSerialization();
    }

    @Test
    public void testSerialization() {
        HeartbeatMessage heartbeatMessage = new HeartbeatMessage();
        byte[] bytes = messageSerialization.serialize(heartbeatMessage);

        // magic + version 0 + message type 0
        byte[] expected = new byte[MessageSerialization.MAGIC.length + 2];
        for (int i = 0; i < MessageSerialization.MAGIC.length; i++) {
            expected[i] = MessageSerialization.MAGIC[i];
        }
        expected[MessageSerialization.MAGIC.length] = MessageSerialization.VERSION;
        expected[MessageSerialization.MAGIC.length] = MessageType.Heartbeat.getId();

        Assertions.assertArrayEquals(expected, bytes);
    }
}
