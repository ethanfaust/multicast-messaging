package dev.efaust.collab;

import dev.efaust.collab.liveness.HeartbeatMessage;
import dev.efaust.collab.liveness.IpTracker;
import dev.efaust.collab.liveness.PeerRegistry;
import dev.efaust.collab.messaging.Message;
import dev.efaust.collab.messaging.MessageSerialization;
import dev.efaust.collab.messaging.MulticastUDPMessagingLayer;
import dev.efaust.collab.messaging.NamedThreadFactory;
import dev.efaust.collab.paxos.PaxosNode;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Collab {
    private static Logger log = LogManager.getLogger(Collab.class);

    private static final String OPTION_PORT = "port";
    private static final String OPTION_IPV4 = "ipv4";
    private static final String OPTION_HELP = "help";

    private static final int PORT_DEFAULT = 4447;

    // IPv4: Pick anything in 224.0.0.0 to 224.0.0.255
    // https://en.wikipedia.org/wiki/Multicast_address#IPv4
    private static final String IPV4_DEFAULT_ADDRESS = "224.69.69.43";

    // https://en.wikipedia.org/wiki/Multicast_address#IPv6
    private static final String IPV6_DEFAULT_ADDRESS = "ff02::1";

    private static long DISCOVERY_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(3);
    private static String SERVICE_NAME = "Collab";

    public static void main(String[] args) {
        log.info("Hello!");
        Configurator.initialize(new DefaultConfiguration());
        Configurator.setRootLevel(Level.INFO);
        try {
            new Collab().run(args);
        } catch (Exception e) {
            log.error("Unhandled exception", e);
        }
    }

    private final Random random;
    private MulticastUDPMessagingLayer multicast;
    private PaxosNode paxosNode;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private IpTracker ipTracker;

    public Collab() {
        random = new Random();
    }

    // Paxos via multicast
    // Similar to mDNS, but uses its own multicast protocol to add new message types
    // https://en.wikipedia.org/wiki/Multicast_DNS

    private void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(SERVICE_NAME, options);
    }

    public void run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("p", OPTION_PORT, true, "udp port to listen on");
        options.addOption("4", OPTION_IPV4, false, "use IPv4");
        options.addOption("h", OPTION_HELP);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("error parsing args", e);
            printUsage(options);
            System.exit(1);
        }

        // important: make sure local firewall allows traffic on this port
        // otherwise you will be able to send but not receive
        // debug with sudo tcpdump 'port 4447'
        // ubuntu: sudo ufw allow 4447/udp
        // fedora: sudo firewall-cmd --add-port 4447/udp
        int port = PORT_DEFAULT;
        boolean useIpv6 = true;

        if (cmd.hasOption(OPTION_HELP)) {
            printUsage(options);
            System.exit(1);
        }
        if (cmd.hasOption(OPTION_PORT)) {
            port = Integer.parseInt(cmd.getOptionValue(OPTION_PORT));
        }
        if (cmd.hasOption(OPTION_IPV4)) {
            useIpv6 = false;
        }

        // could make this configurable... doesn't really matter since network local multicast by convention uses
        // a single address (IPv6), with any port
        String multicastGroupAddress = useIpv6 ? IPV6_DEFAULT_ADDRESS : IPV4_DEFAULT_ADDRESS;

        execute(multicastGroupAddress, port);
    }

    private void receivedHeartbeat(HeartbeatMessage heartbeat) {
        ipTracker.receivedHeartbeat(heartbeat);
    }

    private Runnable getSendRunnable() {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        HeartbeatMessage heartbeat = new HeartbeatMessage();
                        heartbeat.setUuid(random.nextLong());
                        ipTracker.aboutToSendHeartbeat(heartbeat);
                        multicast.send(heartbeat);
                        log.debug("sent");
                    } catch (IOException e) {
                        log.error("Exception on send: {}", e.getMessage());
                    }
                    try {
                        Thread.sleep(DISCOVERY_PERIOD_MILLIS);
                    } catch (InterruptedException e) {

                    }
                }
            }
        };
    }

    private Runnable getReportPeerListRunnable() {
        return new Runnable() {
            @Override
            public void run() {
                log.info("peer list");
                PeerRegistry peerRegistry = paxosNode.getPeerRegistry();
                for (String peer : peerRegistry.getPeers()) {
                    DateTime lastHeartbeat = peerRegistry.getLastHeartbeatTimeForPeer(peer);
                    log.info("peer {} last heartbeat {}", peer, lastHeartbeat);
                }
                if (peerRegistry.getPeers().size() > 0 && !started.get()) {
                    started.set(true);
                    try {
                        paxosNode.sendPrepare(() -> 4L);
                    } catch (IOException e) {
                        log.error("failed to send prepare", e);
                    }
                }
            }
        };
    }

    public void execute(String ip, int port) throws IOException {
        MessageSerialization messageSerialization = new MessageSerialization();
        multicast = new MulticastUDPMessagingLayer(ip, port, messageSerialization);
        paxosNode = new PaxosNode("localhost", multicast);
        multicast.setup();

        // TODO: this will affect the whole round, probably need to find a better solution
        ipTracker = new IpTracker((String determinedIp) -> paxosNode.setNodeId(determinedIp));

        log.info("starting receive packets thread");
        NamedThreadFactory receiveThreadFactory = new NamedThreadFactory("receive");
        Thread receivePacketsThread = receiveThreadFactory.newThread(() -> multicast.run());
        receivePacketsThread.start();
        log.info("receive thread started");

        log.info("starting send thread");
        Thread sendThread = new Thread(getSendRunnable());
        sendThread.setName("send");
        sendThread.start();
        log.info("send thread started");

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("report"));
        scheduledExecutorService.scheduleAtFixedRate(getReportPeerListRunnable(), 0, 10, TimeUnit.SECONDS);

        // runs in main thread
        handlePaxosMessagesLoop();
    }

    private void handlePaxosMessagesLoop() throws IOException {
        Queue<Message> receiveQueue = multicast.getReceiveQueue();
        while (true) {
            Message message = receiveQueue.poll();
            if (message != null) {
                log.debug("received message {} from {}", message, message.getSourceAddress());
                if (message instanceof HeartbeatMessage) {
                    HeartbeatMessage heartbeat = HeartbeatMessage.class.cast(message);
                    receivedHeartbeat(heartbeat);
                }
                try {
                    paxosNode.receiveMessage(message);
                } catch (IOException e) {
                    log.error("error receiving message {}", message, e);
                }
            }
            try {
                // don't spin CPU polling for messages
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
