package dev.efaust.collab.paxos;

import com.google.common.collect.ImmutableSet;
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
import java.util.Set;
import java.util.stream.Collectors;

public class PaxosNodeTest {
    private static Logger log = LogManager.getLogger(PaxosNodeTest.class);

    private static final String ADDRESS_A = "A";
    private static final String ADDRESS_B = "B";
    private static final String ADDRESS_C = "C";
    private static final Set<String> ALL = ImmutableSet.of(ADDRESS_A, ADDRESS_B, ADDRESS_C);

    private InMemoryMessagingLayer msgA;
    private InMemoryMessagingLayer msgB;
    private InMemoryMessagingLayer msgC;
    private PaxosNode a;
    private PaxosNode b;
    private PaxosNode c;
    private InMemoryInterconnect interconnect;

    @BeforeAll
    public static void beforeAll() {
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.INFO);
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        // Set up nodes
        msgA = new InMemoryMessagingLayer(ADDRESS_A);
        a = new PaxosNode(ADDRESS_A, msgA);
        msgB = new InMemoryMessagingLayer(ADDRESS_B);
        b = new PaxosNode(ADDRESS_B, msgB);
        msgC = new InMemoryMessagingLayer(ADDRESS_C);
        c = new PaxosNode(ADDRESS_C, msgC);

        // Set up network
        interconnect = new InMemoryInterconnect();
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
    }

    @Test
    public void testStandardPrepare() throws IOException {
        // send prepare to kick off a round
        long executionId = a.sendPrepare();
        interconnect.drainQueues();

        // receive prepare, process, send promise
        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        // Assert that each node received a prepare message from A
        Set<String> nodesThatReceivedPrepare = interconnect.getHistory().stream()
                .filter((entry) -> entry.getSrcNode().equals(ADDRESS_A))
                .filter((entry) -> entry.getMessage() instanceof PrepareMessage)
                .map((entry) -> entry.getDstNode())
                .collect(Collectors.toSet());
        Assertions.assertEquals(ALL, nodesThatReceivedPrepare);

        log.info("prepare complete");

        // assert internal state updated correctly for each node
        Assertions.assertEquals(1, a.getExecutionState(executionId).getPriorPrepareN());
        Assertions.assertEquals(1, b.getExecutionState(executionId).getPriorPrepareN());
        Assertions.assertEquals(1, c.getExecutionState(executionId).getPriorPrepareN());

        // send promise messages
        interconnect.drainQueues();

        // Assert that A received promise messages from all nodes
        Set<String> nodesThatSentPromise = interconnect.getHistory().stream()
                .filter((entry) -> entry.getDstNode().equals(ADDRESS_A))
                .filter((entry) -> entry.getMessage() instanceof PromiseMessage)
                .map((entry) -> entry.getSrcNode())
                .collect(Collectors.toSet());
        Assertions.assertEquals(ALL, nodesThatSentPromise);
    }

    @Test
    public void testOneExecution() throws IOException {
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

        Set<String> nodesThatSentAccepted = interconnect.getHistory().stream()
                .filter((entry) -> entry.getMessage() instanceof AcceptedMessage)
                .map((entry) -> entry.getSrcNode())
                .collect(Collectors.toSet());
        Assertions.assertEquals(ImmutableSet.of(ADDRESS_A, ADDRESS_B, ADDRESS_C), nodesThatSentAccepted);
    }
}
