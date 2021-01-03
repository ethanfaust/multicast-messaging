package dev.efaust.collab;

import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.messaging.InMemoryInterconnect;
import dev.efaust.collab.messaging.InMemoryMessagingLayer;
import dev.efaust.collab.messaging.MessageHistoryEntry;
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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Repl {
    private static final Logger log = LogManager.getLogger(Repl.class);

    private InMemoryInterconnect interconnect;
    private Map<Integer, InMemoryMessagingLayer> messagingLayers = new HashMap<>();
    private Map<Integer, PaxosNode> nodes = new HashMap<>();

    public static void main(String[] args) {
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.INFO);
        new Repl().run();
    }

    private int getNextNodeId() {
        return nodes.keySet().stream().max(Integer::compare).orElse(0) + 1;
    }

    private String nodeAddress(int nodeId) {
        return Integer.toString(nodeId);
    }

    public void create() {
        int id = getNextNodeId();
        String address = nodeAddress(id);
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

    Pattern STEP_COMMAND = Pattern.compile("^([0-9]+)$");
    public void step(Integer nodeId) throws IOException {
        nodes.get(nodeId).receiveMessages();
        interconnect.drainQueues();
    }

    Pattern NEW_COMMAND = Pattern.compile("^([0-9]+).new$");
    public void prepareNew(Integer nodeId) throws IOException {
        nodes.get(nodeId).sendPrepare(() -> (long)nodeId);
        interconnect.drainQueues();
    }

    Pattern PREPARE_COMMAND = Pattern.compile("^([0-9]+).prepare$");
    public void prepare(Integer nodeId) throws IOException {
        nodes.get(nodeId).sendPrepare(1, () -> (long)nodeId);
        interconnect.drainQueues();
    }

    private void printHistoryEntries(List<MessageHistoryEntry> entries) {
        int i = 0;
        for (MessageHistoryEntry entry : entries) {
            System.out.println(String.format("[%d] %s → %s %s", i, entry.getSrcNode(), entry.getDstNode(), entry.getMessage()));
            i++;
        }
    }

    Pattern SENT_COMMAND = Pattern.compile("^([0-9]+).sent$");
    public void sent(Integer nodeId) throws IOException {
        List<MessageHistoryEntry> entries = interconnect.getHistory().stream()
                .filter((e) -> nodeAddress(nodeId).equals(e.getSrcNode()))
                .collect(Collectors.toList());
        printHistoryEntries(entries);
    }

    Pattern RECEIVED_COMMAND = Pattern.compile("^([0-9]+).received$");
    public void received(Integer nodeId) throws IOException {
        List<MessageHistoryEntry> entries = interconnect.getHistory().stream()
                .filter((e) -> nodeAddress(nodeId).equals(e.getDstNode()))
                .collect(Collectors.toList());
        printHistoryEntries(entries);
    }

    Pattern MESSAGES_COMMAND = Pattern.compile("^([0-9]+).messages$");
    public void messages(Integer nodeId) throws IOException {
        List<MessageHistoryEntry> entries = interconnect.getHistory().stream()
                .filter((e) -> nodeAddress(nodeId).equals(e.getSrcNode()) || nodeAddress(nodeId).equals(e.getDstNode()))
                .collect(Collectors.toList());
        printHistoryEntries(entries);
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
                } else if ((matcher = NEW_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    prepareNew(nodeId);
                } else if ((matcher = PREPARE_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    prepare(nodeId);
                } else if ((matcher = SENT_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    sent(nodeId);
                } else if ((matcher = RECEIVED_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    received(nodeId);
                } else if ((matcher = MESSAGES_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    messages(nodeId);
                }
            } catch (Exception e) {
                log.error("unhandled exception", e);
            }
        }
    }
}
