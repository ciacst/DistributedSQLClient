package org.example.api;
import org.example.util.TableRouter;

public class MasterClientServiceImpl implements MasterClientService {

    private TableRouter router;
    @Override
    public GetRegionServerResp GetRegionServer(String SQL) {
        String region = router.getRegionAndExecute(SQL);
        System.out.println("region: " + region);
        if(region == null || region.equals(""))
            return new GetRegionServerResp("", "", false);
        String peers = router.getRegionPeers(region);
        System.out.println("peers: " + peers);
        if(peers == null || peers.equals(""))
            return new GetRegionServerResp("", "", false);
        return new GetRegionServerResp(region, peers, true);
    }

    public MasterClientServiceImpl() {
        router = new TableRouter();
    }
}
