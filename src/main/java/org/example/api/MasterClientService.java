package org.example.api;
import org.example.api.GetRegionServerResp;
public interface MasterClientService {
    GetRegionServerResp GetRegionServer(String SQL);
}