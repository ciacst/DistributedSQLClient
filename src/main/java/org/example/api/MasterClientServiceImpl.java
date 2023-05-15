package org.example.api;
import org.example.util.TableRouter;

import java.util.List;
import java.util.Map;

public class MasterClientServiceImpl implements MasterClientService {

    private TableRouter router;

    private String fillSpaces(String str) {
        if (str.length() >= 20) {
            return str;
        } else {
            StringBuilder sb = new StringBuilder(str);
            int numOfSpaces = 20 - str.length();
            for (int i = 0; i < numOfSpaces; i++) {
                sb.append(" ");
            }
            return sb.toString();
        }
    }
    
    private String getPairLine(String Table, String Region) {
        return "|" + fillSpaces(Table) + "|" + fillSpaces(Region) + "|\n";
    }
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

    @Override
    public String GetAllTables() {
        String line = "-------------------------------------------\n";
        String ret = line;
        ret += getPairLine("Table", "Region");
        ret += line;
        for(Map.Entry<String, String> entry : router.getTableRegion().entrySet()) {
            ret += getPairLine(entry.getKey(), entry.getValue());
            ret += line;
        }
        return ret;
    }

    @Override
    public String GetRegionPeers() {
        String ret = "Region and peers:\n";
        for(Map.Entry<String, List<String>> entry : router.getRegions().entrySet()) {
            String region = entry.getKey();
            String thisLine = region + ": " + router.getRegionPeers(region) + "\n";
            ret += thisLine;
        }
        return ret;
    }
    
    public MasterClientServiceImpl() {
        router = new TableRouter();
    }
}
