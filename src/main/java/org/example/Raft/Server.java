package org.example.Raft;


import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Class to start a ratis arithmetic example server.
 */

public class Server extends SubCommandBase {
    private final String id;
    private final File storageDir;

    public Server(String raftGroupIdInput, String peersInput, String id,File storageDir){
        super(raftGroupIdInput,peersInput);
        this.id = id;
        this.storageDir = storageDir;

        System.out.println("id: " + id);
        System.out.println("storageDir: " + storageDir.getPath());
    }

    @Override
    public void run() throws Exception {
        RaftPeerId peerId = RaftPeerId.valueOf(id);
        RaftProperties properties = new RaftProperties();

        final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);

        Optional.ofNullable(getPeer(peerId).getClientAddress()).ifPresent(address ->
                GrpcConfigKeys.Client.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
        Optional.ofNullable(getPeer(peerId).getAdminAddress()).ifPresent(address ->
                GrpcConfigKeys.Admin.setPort(properties, NetUtils.createSocketAddr(address).getPort()));

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
        SQLStateMachine stateMachine = new SQLStateMachine();

        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
                getPeers());

        // 设置Extended No Leader TimeOut
        RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties, TimeDuration.valueOf(30,TimeUnit.SECONDS));
        RaftServer raftServer = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(id))
                .setStateMachine(stateMachine).setProperties(properties)
                .setGroup(raftGroup)
                .build();
        raftServer.start();

        for(; raftServer.getLifeCycleState() != LifeCycle.State.CLOSED;) {
            TimeUnit.SECONDS.sleep(1);
        }
    }

}