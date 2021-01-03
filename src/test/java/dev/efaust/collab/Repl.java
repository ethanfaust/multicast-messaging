package dev.efaust.collab;

import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.messaging.InMemoryInterconnect;
import dev.efaust.collab.messaging.InMemoryMessagingLayer;
import dev.efaust.collab.messaging.MessagingLayer;
import dev.efaust.collab.paxos.PaxosNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Repl {
    private static Logger log = LogManager.getLogger(Repl.class);

    private static final String ADDRESS_A = "A";
    private static final String ADDRESS_B = "B";
    private static final String ADDRESS_C = "C";

    private InMemoryMessagingLayer msgA;
    private InMemoryMessagingLayer msgB;
    private InMemoryMessagingLayer msgC;
    private PaxosNode a;
    private PaxosNode b;
    private PaxosNode c;
    private InMemoryInterconnect interconnect;

    public static void main(String[] args) {
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.INFO);
        new Repl().run();
    }

    public Repl() {

    }

    private Map<Integer, InMemoryMessagingLayer> messagingLayers = new HashMap<>();
    private Map<Integer, PaxosNode> nodes = new HashMap<>();

    private int getNextNodeId() {
        return nodes.keySet().stream().max(Integer::compare).orElse(0) + 1;
    }

    public void create() {
        int id = getNextNodeId();
        String address = Integer.toString(id);
        InMemoryMessagingLayer messagingLayer = new InMemoryMessagingLayer(address);
        PaxosNode node = new PaxosNode(address, messagingLayer);
        nodes.put(id, node);
        messagingLayers.put(id, messagingLayer);
        interconnect.addNode(messagingLayer);
        log.info("created node with id {}", id);
    }

    public void heartbeats() throws IOException {
        for (PaxosNode node : nodes.values()) {
            node.sendMessage(new HeartbeatMessage());
        }
        interconnect.drainQueues();
        log.info("heartbeats transmitted from all nodes");
    }

    Pattern PREPARE_COMMAND = Pattern.compile("^([0-9]+).prepare$");
    public void prepare(Integer nodeId) throws IOException {
        nodes.get(nodeId).sendPrepare(() -> (long)nodeId);
        interconnect.drainQueues();
    }

    Pattern STEP_COMMAND = Pattern.compile("^([0-9]+)$");
    public void step(Integer nodeId) throws IOException {
        nodes.get(nodeId).receiveMessages();
        interconnect.drainQueues();
    }

    public void help() {
        System.out.println("paxos repl");
        System.out.println("  available commands:");
        System.out.println("      create - create new node");
        System.out.println("      heartbeats - send heartbeats from all nodes");
        System.out.println("      N (e.g. 1) - receive messages for node N, process");
        System.out.println("      N.prepare (e.g. 1.prepare) - send prepare from node N");
    }

    public void run() {
        interconnect = new InMemoryInterconnect();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        Matcher matcher;

        while (true) {
            System.out.print("> ");
            try {
                String input = reader.readLine().strip();

                if ("help".equals(input)) {
                    help();
                } else if ("create".equals(input)) {
                    create();
                } else if ("heartbeats".equals(input)) {
                    heartbeats();
                } else if ((matcher = STEP_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    MessagingLayer messagingLayer = messagingLayers.get(nodeId);

                    if (messagingLayer.getReceiveQueue().isEmpty()) {
                        System.out.println(String.format("[%d] receive queue empty", nodeId));
                    } else {
                        System.out.println(String.format("[%d] receive queue", nodeId));
                        messagingLayer.getReceiveQueue().stream().forEachOrdered(msg -> {
                            System.out.println(String.format("  %s", msg));
                        });

                        System.out.println("  (press enter to continue)");
                        // TODO: add interactive option for message reordering / dropping instead
                        reader.readLine();
                    }

                    step(nodeId);
                } else if ((matcher = PREPARE_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    prepare(nodeId);
                }
            } catch (Exception e) {
                log.error("unhandled exception", e);
            }
        }
    }
}
