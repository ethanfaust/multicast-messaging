package dev.efaust.collab.paxos;

import com.google.common.collect.ImmutableSet;
import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.messaging.InMemoryInterconnect;
import dev.efaust.collab.messaging.InMemoryMessagingLayer;
import dev.efaust.collab.messaging.Message;
import dev.efaust.collab.messaging.MessageHistoryEntry;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PaxosNodeTest {
    private static final Logger log = LogManager.getLogger(PaxosNodeTest.class);

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
        long executionId = a.sendPrepare(() -> 2L);
        interconnect.drainQueues();

        // receive prepare, process, send promise
        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        // Assert that each node received a prepare message from A
        Set<String> nodesThatReceivedPrepare = interconnect.getHistory().stream()
                .filter((entry) -> entry.getSrcNode().equals(ADDRESS_A))
                .filter((entry) -> entry.getMessage() instanceof PrepareMessage)
                .map(MessageHistoryEntry::getDstNode)
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
                .map(MessageHistoryEntry::getSrcNode)
                .collect(Collectors.toSet());
        Assertions.assertEquals(ALL, nodesThatSentPromise);
    }

    // note: this will also drop messages if the message source is not in the list
    protected void reorderReceiveQueueBySrc(InMemoryMessagingLayer messagingLayer, List<String> desiredOrder) {
        Queue<Message> receiveQueue = messagingLayer.getReceiveQueue();
        List<Message> messages = receiveQueue.stream().collect(Collectors.toList());
        List<Message> newOrder = new ArrayList<>();
        for (String nodeId : desiredOrder) {
            messages.stream()
                    .filter((msg) -> nodeId.equals((msg.getSourceAddress())))
                    .forEach((msg) -> newOrder.add(msg));
        }
        messagingLayer.getReceiveQueue().clear();
        for (Message msg : newOrder) {
            receiveQueue.add(msg);
        }
    }

    protected void setupConflictingPrepare() throws IOException {
        // This test represents 2 roughly concurrent requests triggering different N.
        // Depending on deliver order of messages / where faults occur, outcome will be different.

        long executionId = a.getNextExecutionId();
        a.sendPrepare(executionId, () -> 99L); // N=1
        interconnect.drainQueues();
        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();
        b.sendPrepare(executionId, () -> 42L); // N=2
        interconnect.drainQueues();

        // who should win? A or B?  -> depends on timing of accepts for A vs. promises for B

        c.receiveMessages();
        b.receiveMessages();
        a.receiveMessages();

        interconnect.drainQueues();

        // [B] sent <PleaseAccept src='null' executionId='1', N='1' V='42' />
        // [A] sent <PleaseAccept src='null' executionId='1', N='1' V='99' />
        // [A] sent <Promise src='null' executionId='1', promiseN='2' priorAcceptedN='-1' priorAcceptedValue='-1' />
    }

    @Test
    public void testConflictingPrepareSequence1() throws IOException {
        setupConflictingPrepare();

        // One possible ending sequence:
        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        interconnect.drainQueues();

        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        interconnect.drainQueues();

        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        // Accepted 99 (depends on delivery order)
        Assertions.assertTrue(true);
    }

    @Test
    public void testConflictingPrepareSequence2() throws IOException {
        setupConflictingPrepare();

        // alternate ending sequence:
        b.receiveMessages();

        interconnect.drainQueues();

        b.receiveMessages();

        interconnect.drainQueues();

        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();

        interconnect.drainQueues();

        a.receiveMessages();
        b.receiveMessages();
        c.receiveMessages();
        // Accepted 42, N=2

        interconnect.drainQueues();

        log.info("receive queue sizes: A: {}, B: {}, C: {}",
                msgA.getReceiveQueue().size(),
                msgB.getReceiveQueue().size(),
                msgC.getReceiveQueue().size());

        // 42 has won, it has been accepted by a majority (A, C), N=2

        // TODO: figure out some better way to make assertions on outcome
        // Message ordering/delivery depends on implementation of the messaging layer.
        // This test should not be coupled to that.

        // Perhaps validate message history instead?

        // TODO: Temporary placeholder, this should be removed:
        Assertions.assertTrue(true);
    }

    @Test
    public void testOneExecution() throws IOException {
        // send prepare to kick off a round
        long executionId = a.sendPrepare(() -> 4L);
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
                .map(MessageHistoryEntry::getSrcNode)
                .collect(Collectors.toSet());
        Assertions.assertEquals(ImmutableSet.of(ADDRESS_A, ADDRESS_B, ADDRESS_C), nodesThatSentAccepted);
    }
}
