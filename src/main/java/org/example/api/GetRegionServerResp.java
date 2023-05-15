package org.example.api;

import java.io.Serializable;

public class GetRegionServerResp implements Serializable {
    public String Region;
    public String Peers;

    public boolean Found;

    public GetRegionServerResp(String region, String peers, boolean found) {
        Region = region;
        Peers = peers;
        Found = found;
    }
}
