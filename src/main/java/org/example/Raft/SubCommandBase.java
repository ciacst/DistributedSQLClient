package org.example.Raft;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Base subcommand class which includes the basic raft properties.
 */
public abstract class SubCommandBase {
    private String raftGroupId;
    private String peers;
    public SubCommandBase(String raftGroupIdInput, String peersInput){
        raftGroupId = raftGroupIdInput;
        peers = peersInput;
    }

    public static RaftPeer[] parsePeers(String peers) {
        return Stream.of(peers.split(",")).map(address -> {
            String[] addressParts = address.split(":");
            if (addressParts.length < 3) {
                throw new IllegalArgumentException(
                        "Raft peer " + address + " is not a legitimate format. "
                                + "(format: name:host:port:dataStreamPort:clientPort:adminPort)");
            }
            RaftPeer.Builder builder = RaftPeer.newBuilder();
            builder.setId(addressParts[0]).setAddress(addressParts[1] + ":" + addressParts[2]);

            return builder.build();
        }).toArray(RaftPeer[]::new);
    }

    public RaftPeer[] getPeers() {
        return parsePeers(peers);
    }

    public RaftPeer getPrimary() {
        return parsePeers(peers)[0];
    }

    public abstract void run() throws Exception;

    public String getRaftGroupId() {
        return raftGroupId;
    }

    public RoutingTable getRoutingTable(Collection<RaftPeer> raftPeers, RaftPeer primary) {
        RoutingTable.Builder builder = RoutingTable.newBuilder();
        RaftPeer previous = primary;
        for (RaftPeer peer : raftPeers) {
            if (peer.equals(primary)) {
                continue;
            }
            builder.addSuccessor(previous.getId(), peer.getId());
            previous = peer;
        }

        return builder.build();
    }

    /**
     * @return the peer with the given id if it is in this group; otherwise, return null.
     */
    public RaftPeer getPeer(RaftPeerId raftPeerId) {
        Objects.requireNonNull(raftPeerId, "raftPeerId == null");
        for (RaftPeer p : getPeers()) {
            if (raftPeerId.equals(p.getId())) {
                return p;
            }
        }
        throw new IllegalArgumentException("Raft peer id " + raftPeerId + " is not part of the raft group definitions " +
                this.peers);
    }
}
