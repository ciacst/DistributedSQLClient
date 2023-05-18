package org.example;


import org.example.client.client;
import org.example.master.MasterServer;
import org.example.region.RegionServer;

import java.io.*;



public class Main {

    public static void main(String[] args) throws IOException {
        RegionServer mst = new RegionServer();
        mst.run();
    }
}