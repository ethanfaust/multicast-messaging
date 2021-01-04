package dev.efaust.collab.liveness;

import com.google.common.collect.EvictingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Uses heartbeat messages to track IP of this node.
 */
public class IpTracker {
    private static Logger log = LogManager.getLogger(IpTracker.class);

    // TODO: make sure we allow heartbeats to be dropped
    private static final int PRIOR_HEARTBEATS_TO_REMEMBER = 10;
    private static final int OVERLAP_THRESHOLD = 3;

    private EvictingQueue<Long> uuidsSentOrder;

    class ReceivedEntry {
        long uuid;
        String ip;

        public ReceivedEntry(long uuid, String ip) {
            this.uuid = uuid;
            this.ip = ip;
        }
    }
    private EvictingQueue<ReceivedEntry> matchingUuidsReceived;

    private Optional<String> ip;

    private Consumer<String> ipConsumer;

    public IpTracker(Consumer<String> ipConsumer) {
        uuidsSentOrder = EvictingQueue.create(PRIOR_HEARTBEATS_TO_REMEMBER);
        matchingUuidsReceived = EvictingQueue.create(PRIOR_HEARTBEATS_TO_REMEMBER);
        this.ipConsumer = ipConsumer;
    }

    public synchronized void aboutToSendHeartbeat(HeartbeatMessage heartbeat) {
        long uuid = heartbeat.getUuid();
        uuidsSentOrder.add(uuid);
    }

    public synchronized void receivedHeartbeat(HeartbeatMessage heartbeat) {
        long uuid = heartbeat.getUuid();

        if (uuidsSentOrder.contains(uuid)) {
            matchingUuidsReceived.add(new ReceivedEntry(heartbeat.uuid, heartbeat.getSourceAddress()));
            checkIfIpDetermined();
        }
    }

    public void checkIfIpDetermined() {
        Set<Long> sent = uuidsSentOrder.stream().collect(Collectors.toSet());
        Map<String, Integer> ipCounts = new HashMap<>();
        matchingUuidsReceived.stream()
                .filter((entry) -> sent.contains(entry.uuid))
                .forEach((entry) -> ipCounts.merge(entry.ip, 1, (a, b) -> a + b));
        Optional<Map.Entry<String, Integer>> entryWithMaxOverlap = ipCounts.entrySet().stream()
                .max(Comparator.comparing(Map.Entry::getValue));
        if (entryWithMaxOverlap.isPresent() && entryWithMaxOverlap.get().getValue() > OVERLAP_THRESHOLD) {
            ip = Optional.of(entryWithMaxOverlap.get().getKey());
            log.info("ip {} confidence {}/{}", ip.get(), entryWithMaxOverlap.get().getValue(), matchingUuidsReceived.stream().count());
            ipConsumer.accept(ip.get());
        }
    }
}
