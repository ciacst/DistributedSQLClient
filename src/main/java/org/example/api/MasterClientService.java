package org.example.api;
import org.example.api.GetRegionServerResp;
public interface MasterClientService {
    GetRegionServerResp GetRegionServer(String SQL);

    String GetAllTables();

    String GetRegionPeers();
}