package dev.efaust.collab.messaging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.util.Arrays;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class MulticastUDPMessagingLayer implements MessagingLayer, Runnable {
    Logger log = LogManager.getLogger(MulticastUDPMessagingLayer.class);

    private static final int MESSAGE_BUFFER_SIZE_BYTES = 256;

    private String ip;
    private int port;
    private Queue<Message> receiveQueue;
    private MulticastSocket socket;
    private InetAddress groupAddress;
    private InetSocketAddress socketAddress;
    private MessageSerialization serializationLayer;

    public MulticastUDPMessagingLayer(String ip, int port, MessageSerialization serializationLayer) {
        this.ip = ip;
        this.port = port;
        this.serializationLayer = serializationLayer;
        this.receiveQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public Queue<Message> getReceiveQueue() {
        return receiveQueue;
    }

    @Override
    public void run() {
        if (socket == null) {
            throw new IllegalStateException("must call setup before run");
        }
        byte[] buf = new byte[MESSAGE_BUFFER_SIZE_BYTES];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        log.info("starting receive");
        while (true) {
            try {
                socket.receive(packet);
                receivedPacket(buf, packet.getLength(), packet.getAddress());
                packet.setLength(buf.length);
            } catch (IOException e) {
                log.error("error receiving: {}", e.getMessage());
            }
        }
        //shutdown();
    }

    private void receivedPacket(byte[] bytes, int length, InetAddress src) throws IOException {
        log.debug("received packet " + MessageSerialization.bytesArrayToString(bytes, length) + " from " + src);
        byte[] packetBytes = Arrays.copyOfRange(bytes, 0, length);
        boolean success = false;
        if (serializationLayer.validate(packetBytes)) {
            Optional<Message> deserializedMessage = serializationLayer.deserialize(packetBytes);
            if (deserializedMessage.isPresent()) {
                success = true;
                Message message = deserializedMessage.get();
                message.setSourceAddress(src.getHostAddress());
                receiveQueue.add(message);
            }
        }
        if (!success) {
            log.warn("invalid packet");
        }
    }

    protected void shutdown() throws IOException {
        socket.leaveGroup(socketAddress, null);
        socket.close();
    }

    public void setup() throws IOException {
        this.socket = new MulticastSocket(port);
        socket.setReuseAddress(true);
        groupAddress = InetAddress.getByName(ip);
        log.info("using multicast group address {}, port {}", groupAddress.getHostAddress(), port);
        log.info("note: if this node is not receiving messages, please validate that {}/udp is allowed through your firewall", port);
        socketAddress = new InetSocketAddress(groupAddress, port);
        socket.joinGroup(socketAddress, null);
        log.info("setup complete");
    }

    private void send(byte[] bytes) throws IOException {
        if (socket == null) {
            throw new IllegalStateException("must call setup before send");
        }
        if (bytes == null || bytes.length == 0) {
            throw new IOException("bytes should not be null or of length 0");
        }
        DatagramPacket datagramPacket = new DatagramPacket(bytes, bytes.length, groupAddress, port);
        socket.send(datagramPacket);
    }

    public void send(Message message) throws IOException {
        byte[] bytes = serializationLayer.serialize(message);
        send(bytes);
    }
}
