package dev.efaust.collab.liveness;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PeerRegistry {
    private Map<String, DateTime> peerLastHeartbeatTime;

    public PeerRegistry() {
        peerLastHeartbeatTime = new HashMap<>();
    }

    public void updatePeerHeartbeat(String peer, DateTime time) {
        peerLastHeartbeatTime.put(peer, time);
    }

    public Set<String> getPeers() {
        return peerLastHeartbeatTime.keySet();
    }

    public DateTime getLastHeartbeatTimeForPeer(String peer) {
        return peerLastHeartbeatTime.get(peer);
    }

    public Set<String> peersAlive() {
        // TODO: add liveness threshold
        return peerLastHeartbeatTime.keySet();
    }
}
