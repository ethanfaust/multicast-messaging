package dev.efaust.collab.paxos;

import dev.efaust.collab.messaging.InMemoryInterconnect;
import dev.efaust.collab.messaging.InMemoryMessagingLayer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.junit.jupiter.api.*;

import java.io.IOException;

public class OneExecutionTest {
    private static final String ADDRESS_A = "192.168.1.10";
    private static final String ADDRESS_B = "192.168.1.20";
    private static final String ADDRESS_C = "192.168.1.30";

    @BeforeAll
    public static void beforeAll() {
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.INFO);
    }

    @Test
    public void testOneExecution() throws IOException {
        InMemoryMessagingLayer msgA = new InMemoryMessagingLayer(ADDRESS_A);
        PaxosNode a = new PaxosNode(ADDRESS_A, msgA);

        InMemoryMessagingLayer msgB = new InMemoryMessagingLayer(ADDRESS_B);
        PaxosNode b = new PaxosNode(ADDRESS_B, msgB);

        InMemoryMessagingLayer msgC = new InMemoryMessagingLayer(ADDRESS_C);
        PaxosNode c = new PaxosNode(ADDRESS_C, msgC);

        InMemoryInterconnect interconnect = new InMemoryInterconnect();
        interconnect.addNode(msgA);
        interconnect.addNode(msgB);
        interconnect.addNode(msgC);

        a.sendMessage(new PrepareMessage(0, 42));
        interconnect.drainQueues();

        // triggers promise response from A
        a.receiveMessages();

        // triggers promise response from B
        b.receiveMessages();

        // triggers promise response from C
        c.receiveMessages();

        // assert internal state updated correctly for each node
        Assertions.assertEquals(42, a.getExecutionState(0).getPriorPrepareN());
        Assertions.assertEquals(42, b.getExecutionState(0).getPriorPrepareN());
        Assertions.assertEquals(42, c.getExecutionState(0).getPriorPrepareN());

        interconnect.drainQueues();

        a.receiveMessages();
        // TODO: assert promises sent, etc.
    }
}
