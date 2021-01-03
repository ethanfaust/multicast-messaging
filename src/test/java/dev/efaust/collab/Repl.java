package dev.efaust.collab;

import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.messaging.*;
import dev.efaust.collab.paxos.PaxosNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Repl {
    private static final Logger log = LogManager.getLogger(Repl.class);

    private static final String COMMAND_SETUP = "setup";
    private static final String COMMAND_CREATE = "create";
    private static final String COMMAND_HEARTBEATS = "heartbeats";
    private static final String COMMAND_HELP = "help";
    private static final String COMMAND_HISTORY = "history";
    private static final Pattern SENT_COMMAND = Pattern.compile("^([0-9]+).sent$");
    private static final Pattern STEP_COMMAND = Pattern.compile("^([0-9]+)$");
    private static final Pattern NEW_COMMAND = Pattern.compile("^([0-9]+).new$");
    private static final Pattern PREPARE_COMMAND = Pattern.compile("^([0-9]+).prepare$");
    private static final Pattern RECEIVED_COMMAND = Pattern.compile("^([0-9]+).received$");
    private static final Pattern MESSAGES_COMMAND = Pattern.compile("^([0-9]+).messages$");

    private InMemoryInterconnect interconnect;
    private Map<Integer, InMemoryMessagingLayer> messagingLayers = new HashMap<>();
    private Map<Integer, PaxosNode> nodes = new HashMap<>();
    private List<String> commandLog = new ArrayList<>();

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

    public void step(Integer nodeId) throws IOException {
        nodes.get(nodeId).receiveMessages();
        interconnect.drainQueues();
    }

    public void prepareNew(Integer nodeId) throws IOException {
        nodes.get(nodeId).sendPrepare(() -> (long)nodeId);
        interconnect.drainQueues();
    }

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

    public void sent(Integer nodeId) throws IOException {
        List<MessageHistoryEntry> entries = interconnect.getHistory().stream()
                .filter((e) -> nodeAddress(nodeId).equals(e.getSrcNode()))
                .collect(Collectors.toList());
        printHistoryEntries(entries);
    }

    public void received(Integer nodeId) throws IOException {
        List<MessageHistoryEntry> entries = interconnect.getHistory().stream()
                .filter((e) -> nodeAddress(nodeId).equals(e.getDstNode()))
                .collect(Collectors.toList());
        printHistoryEntries(entries);
    }

    public void messages(Integer nodeId) throws IOException {
        List<MessageHistoryEntry> entries = interconnect.getHistory().stream()
                .filter((e) -> nodeAddress(nodeId).equals(e.getSrcNode()) || nodeAddress(nodeId).equals(e.getDstNode()))
                .collect(Collectors.toList());
        printHistoryEntries(entries);
    }

    public void help() {
        System.out.println("*** paxos repl ***");
        System.out.println("available commands:");
        System.out.println("    create - create new node");
        System.out.println("    heartbeats - send heartbeats from all nodes");
        System.out.println("    N (e.g. 1) - receive messages for node N, process");
        System.out.println("        there is an option to interactively reorder/drop messages prior to delivery");
        System.out.println("    N.new (e.g. 1.new) - send prepare from node N for new session");
        System.out.println("    N.prepare (e.g. 1.prepare) - send prepare from node N for existing session");
        System.out.println("    N.sent (e.g. 1.sent) - list all messages sent by node N");
        System.out.println("    N.received (e.g. 1.received) - list all messages sent by node N");
        System.out.println("    N.messages (e.g. 1.messages) - list all messages sent or received by node N");
        System.out.println("    history - list prior mutating commands");

    }

    private void printReceiveQueue(Queue<Message> queue) {
        List<Message> messages = queue.stream().collect(Collectors.toList());
        int i = 0;
        for (Message msg : messages) {
            System.out.println(String.format("  [%d] %s", i, msg));
            i++;
        }
    }

    private void interactiveReorder(BufferedReader reader, Queue<Message> queue) throws IOException {
        System.out.println("enter new order by index, e.g. 2 0 1");
        System.out.println("indices not included will be dropped");
        System.out.print("new order: ");
        String newOrderResponse = reader.readLine().strip();
        List<Integer> newOrder = Arrays.asList(newOrderResponse.split(" ")).stream().map(Integer::parseInt).collect(Collectors.toList());
        List<Message> messages = queue.stream().collect(Collectors.toList());
        queue.clear();
        for (int index : newOrder) {
            queue.add(messages.get(index));
        }
        System.out.println("reordered/dropped messages, new receive queue:");
        printReceiveQueue(queue);
        commandLog.add(String.format("  reorder %s", newOrderResponse));
    }

    private void history() {
        int i = 0;
        for (String command : commandLog) {
            System.out.println(String.format("[%d] %s", i, command));
            i++;
        }
    }

    private void defaultSetup() throws IOException {
        create();
        commandLog.add(COMMAND_CREATE);
        create();
        commandLog.add(COMMAND_CREATE);
        create();
        commandLog.add(COMMAND_CREATE);
        heartbeats();
        commandLog.add(COMMAND_HEARTBEATS);
        step(1);
        commandLog.add("1");
        step(2);
        commandLog.add("2");
        step(3);
        commandLog.add("3");
        log.info("default setup complete, 3 node cluster, heartbeats exchanged");
    }

    public void run() {
        interconnect = new InMemoryInterconnect();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        Matcher matcher;

        help();
        System.out.println("typical invocation: setup / 1.new");

        while (true) {
            System.out.print("> ");
            try {
                String input = reader.readLine().strip();
                boolean log = true;

                if (COMMAND_HELP.equals(input)) {
                    log = false;
                    help();
                } else if (COMMAND_HISTORY.equals(input)) {
                    log = false;
                    history();
                } else if (COMMAND_SETUP.equals(input)) {
                    log = false;
                    defaultSetup();
                } else if (COMMAND_CREATE.equals(input)) {
                    create();
                } else if (COMMAND_HEARTBEATS.equals(input)) {
                    heartbeats();
                } else if ((matcher = STEP_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    MessagingLayer messagingLayer = messagingLayers.get(nodeId);

                    // need to add custom log entries for reordering
                    log = false;
                    commandLog.add(input);

                    if (messagingLayer.getReceiveQueue().isEmpty()) {
                        System.out.println(String.format("[%d] receive queue empty", nodeId));
                    } else {
                        System.out.println(String.format("[%d] receive queue", nodeId));
                        Queue<Message> receiveQueue = messagingLayer.getReceiveQueue();
                        printReceiveQueue(receiveQueue);
                        System.out.print("  reorder/drop messages [N/y]? ");
                        String response = reader.readLine().strip();
                        if (response.equalsIgnoreCase("y")) {
                            interactiveReorder(reader, receiveQueue);
                        }
                    }

                    step(nodeId);
                } else if ((matcher = NEW_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    prepareNew(nodeId);
                } else if ((matcher = PREPARE_COMMAND.matcher(input)).matches()) {
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    prepare(nodeId);
                } else if ((matcher = SENT_COMMAND.matcher(input)).matches()) {
                    log = false;
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    sent(nodeId);
                } else if ((matcher = RECEIVED_COMMAND.matcher(input)).matches()) {
                    log = false;
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    received(nodeId);
                } else if ((matcher = MESSAGES_COMMAND.matcher(input)).matches()) {
                    log = false;
                    Integer nodeId = Integer.parseInt(matcher.group(1));
                    messages(nodeId);
                } else {
                    log = false;
                    System.out.println(String.format("unknown command: %s", input));
                }
                if (log) {
                    commandLog.add(input);
                }

            } catch (Exception e) {
                log.error("unhandled exception", e);
            }
        }
    }
}
