package dev.efaust.collab;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class Collab {
    private static Logger log = LogManager.getLogger(Collab.class);

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

    public void run(String[] args) throws Exception {
        boolean useIpv6 = true;

        // IPv4: Pick anything in 224.0.0.0 to 224.0.0.255
        // https://en.wikipedia.org/wiki/Multicast_address#IPv4
        String ipv4Default = "224.69.69.43";
        // IPv6: ff02::1
        // https://en.wikipedia.org/wiki/Multicast_address#IPv6
        String ipv6Default = "ff02::1";

        String multicastGroupAddress = useIpv6? ipv6Default : ipv4Default;

        // important: make sure local firewall allows traffic on this port
        // otherwise you will be able to send but not receive
        // debug with sudo tcpdump 'port 4447'
        // ubuntu: sudo ufw allow 4447/udp
        // fedora: sudo firewall-cmd --add-port 4447/udp
        int port = 4447;

        MessageSerialization messageSerialization = new MessageSerialization();
        MulticastUDPMessagingLayer multicast = new MulticastUDPMessagingLayer(multicastGroupAddress, port, messageSerialization);
        multicast.setup();

        log.info("starting receive thread");
        Thread receiveThread = new Thread() {
            @Override
            public void run() {
                multicast.run();
            }
        };
        receiveThread.start();
        log.info("receive thread started");

        log.info("starting send thread");
        Thread sendThread = new Thread(){
            @Override
            public void run() {
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

        Queue<Message> receiveQueue = multicast.getReceiveQueue();
        while (true) {
            Message message = receiveQueue.poll();
            if (message != null) {
                log.info("received message {} from {}", message, message.sourceAddress);
            }
        }
    }
}
