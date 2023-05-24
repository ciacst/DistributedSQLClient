package org.example;


import org.example.client.client;
import org.example.master.MasterServer;
import org.example.region.RegionServer;

import java.io.*;



public class Main {

    public static void main(String[] args) throws IOException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");

//        RegionServer mst = new RegionServer();
//        mst.run();

        MasterServer master = new MasterServer();
        master.run();

//        client my_client = new client();
//        my_client.run();

    }
}