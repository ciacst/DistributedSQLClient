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
import java.util.List;

public class MasterRegionServiceImpl implements MasterRegionService {
    TableRouter router;
    public synchronized boolean ReportRegion(String RegionId, String ServerIP) {
        router.addRegionServer(RegionId, ServerIP);
        return true;
    }

    // return true: ok to delete all tables
    // return false：not to delete all tables
    public synchronized boolean ReportFailure(String RegionId, String SQLDump){
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
