package dev.efaust.collab;

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

    public DateTime getLastHeartbeatForPeer(String peer) {
        return peerLastHeartbeatTime.get(peer);
    }
}
