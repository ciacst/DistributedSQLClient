package org.example.Raft;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;

import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * Client to connect cluster.
 */
public class Client extends SubCommandBase {
    private RaftClient client;

    public Client(String raftGroupIdInput, String peersInput) {
        super(raftGroupIdInput, peersInput);
    }

    @Override
    public void run() throws Exception {
        RaftProperties raftProperties = new RaftProperties();

        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
                getPeers());

        RaftClient.Builder builder =
                RaftClient.newBuilder().setProperties(raftProperties);
        builder.setRaftGroup(raftGroup);
        builder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
        client = builder.build();

//        operation(client, sql);


    }

    public String operation(String sql) throws IOException {
        RaftClientReply send = client.io().send(
                new SQLMessage(sql));
        System.out.println("Success: " + send.isSuccess());
        Message res = send.getMessage();
        System.out.println(res.getContent().toStringUtf8());

        return send.getMessage().toString();
    }
}