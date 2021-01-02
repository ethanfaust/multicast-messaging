package dev.efaust.collab.paxos;

import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.messaging.InMemoryInterconnect;
import dev.efaust.collab.messaging.InMemoryMessagingLayer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.junit.jupiter.api.*;

import java.io.IOException;

public class OneExecutionTest {
    private static Logger log = LogManager.getLogger(OneExecutionTest.class);

    private static final String ADDRESS_A = "A";
    private static final String ADDRESS_B = "B";
    private static final String ADDRESS_C = "C";

    @BeforeAll
    public static void beforeAll() {
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.INFO);
    }

    @Test
    public void testOneExecution() throws IOException {
        // Set up nodes
        InMemoryMessagingLayer msgA = new InMemoryMessagingLayer(ADDRESS_A);
        PaxosNode a = new PaxosNode(ADDRESS_A, msgA);
        InMemoryMessagingLayer msgB = new InMemoryMessagingLayer(ADDRESS_B);
        PaxosNode b = new PaxosNode(ADDRESS_B, msgB);
        InMemoryMessagingLayer msgC = new InMemoryMessagingLayer(ADDRESS_C);
        PaxosNode c = new PaxosNode(ADDRESS_C, msgC);

        // Set up network
        InMemoryInterconnect interconnect = new InMemoryInterconnect();
        interconnect.addNode(msgA);
        interconnect.addNode(msgB);
        interconnect.addNode(msgC);

        // Send heartbeats to facilitate neighbor discovery.
        // Later phases of Paxos depend on consensus from a quorum of nodes, thus each node needs to know of the others
        // in the cluster. In the test, we abstract away this problem by making every node aware of each other from the
        // very start.
        a.sendMessage(new HeartbeatMessage());
        b.sendMessage(new HeartbeatMessage());
        c.sendMessage(new HeartbeatMessage());
        interconnect.drainQueues();
        // process heartbeats
        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        log.info("heartbeats complete");

        // send prepare to kick off a round
        long executionId = a.sendPrepare();
        interconnect.drainQueues();

        // receive prepare, process, send promise
        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        log.info("prepare complete");

        // assert internal state updated correctly for each node
        Assertions.assertEquals(1, a.getExecutionState(executionId).getPriorPrepareN());
        Assertions.assertEquals(1, b.getExecutionState(executionId).getPriorPrepareN());
        Assertions.assertEquals(1, c.getExecutionState(executionId).getPriorPrepareN());

        // send promise messages
        interconnect.drainQueues();

        log.info("promise sending complete");

        // receive promise messages, send accept
        a.receiveMessages();
        // TODO: assert promises sent, etc.

        b.receiveMessages();
        c.receiveMessages();

        interconnect.drainQueues();

        // receive accept, send accepted
        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        interconnect.drainQueues();

        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();
    }
}
