package org.example.api;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.example.Raft.Client;
import org.example.util.TableRouter;

import java.io.IOException;
import java.util.List;

public class MasterRegionServiceImpl implements MasterRegionService {
    public static String DeleteAll = "DELETEALLTABLES!";
    TableRouter router;
    public synchronized boolean ReportRegion(String RegionId, String ServerIP) {
        System.out.println("Dubbo received ReportRegion " + RegionId + " "  + ServerIP);
        // 如果表里没Region，说明这个Region被清空了，所以得清空一下Region的数据库
        if(!router.getRegions().containsKey(RegionId)){
            try {
                Client tmpRaftClient = new Client(RegionId,ServerIP);
                tmpRaftClient.operation(DeleteAll);
            } catch (IOException e) {
                e.printStackTrace();
            }

            router.addRegionServer(RegionId, ServerIP);
        }
        return true;
    }

    // return true: ok to delete all tables
    // return false：not to delete all tables
    public synchronized boolean ReportFailure(String RegionId, String SQLDump){
        System.out.println("Dubbo received ReportFailure " + RegionId);
        // 0. check whether this is the last region
        if(router.getRegions().size() == 1){
            return false;
        }

        // 1. delete failed region
        router.deleteRegionServer(RegionId);
        // 2. select best region
        String targetGroupID = router.GetMinRegion();
        String peers = router.getRegionPeers(targetGroupID);
        // 3. send message
        Client tmpRaftClient = new Client(targetGroupID,peers);

        try {
            tmpRaftClient.run();
            tmpRaftClient.operation(SQLDump);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public MasterRegionServiceImpl(TableRouter router_) {
        router = router_;
    }
}
