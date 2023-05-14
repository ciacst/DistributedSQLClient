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
        RaftClientReply send ;

        try {
            // 设置超时时间为10秒钟
            send = client.io().send(
                    new SQLMessage(sql),1000);
        } catch (TimeoutIOException e) {
            // 超时处理
            client.close();
            return "Request timed out";
        } catch (IOException e) {
            // 其他异常处理
            return "Exception occurred: " + e.getMessage();
        }

        return send.getMessage().getContent().toStringUtf8();

        

    }
}