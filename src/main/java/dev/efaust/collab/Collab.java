package dev.efaust.collab;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.*;

public class Collab {
    private static Logger log = LogManager.getLogger(Collab.class);

    private static final String OPTION_PORT = "port";
    private static final String OPTION_IPV4 = "ipv4";
    private static final String OPTION_HELP = "help";

    long DISCOVERY_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(3);
    String SERVICE_NAME = "Collab";

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
        int port = 4447;
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

        // IPv4: Pick anything in 224.0.0.0 to 224.0.0.255
        // https://en.wikipedia.org/wiki/Multicast_address#IPv4
        String ipv4Default = "224.69.69.43";
        // IPv6: ff02::1
        // https://en.wikipedia.org/wiki/Multicast_address#IPv6
        String ipv6Default = "ff02::1";

        String multicastGroupAddress = useIpv6 ? ipv6Default : ipv4Default;
        execute(multicastGroupAddress, port);
    }

    public void execute(String ip, int port) throws IOException {
        PeerRegistry peerRegistry = new PeerRegistry();
        MessageSerialization messageSerialization = new MessageSerialization();
        MulticastUDPMessagingLayer multicast = new MulticastUDPMessagingLayer(ip, port, messageSerialization);
        multicast.setup();

        log.info("starting receive thread");
        Thread receiveThread = new Thread() {
            @Override
            public void run() {
                setName("receive");
                multicast.run();
            }
        };
        receiveThread.start();
        log.info("receive thread started");

        log.info("starting send thread");
        Thread sendThread = new Thread(){
            @Override
            public void run() {
                setName("send");
                while (true) {
                    try {
                        Message heartbeat = new HeartbeatMessage();
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
        sendThread.start();
        log.info("send thread started");

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("report");
                return thread;
            }
        });
        Runnable reporter = new Runnable() {
            @Override
            public void run() {
                log.info("peer list");
                for (String peer : peerRegistry.getPeers()) {
                    DateTime lastHeartbeat = peerRegistry.getLastHeartbeatForPeer(peer);
                    log.info("peer {} last heartbeat {}", peer, lastHeartbeat);
                }
            }
        };
        scheduledExecutorService.scheduleAtFixedRate(reporter, 0, 30, TimeUnit.SECONDS);

        Queue<Message> receiveQueue = multicast.getReceiveQueue();
        while (true) {
            Message message = receiveQueue.poll();
            if (message != null) {
                log.info("received message {} from {}", message, message.sourceAddress);
                if (message instanceof HeartbeatMessage) {
                    peerRegistry.updatePeerHeartbeat(message.sourceAddress, DateTime.now(DateTimeZone.UTC));
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
