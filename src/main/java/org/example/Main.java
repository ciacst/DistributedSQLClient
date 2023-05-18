package org.example;


import org.example.client.client;
import org.example.master.MasterServer;
import java.io.*;



public class Main {

    public static void main(String[] args) throws IOException {
        MasterServer mst = new MasterServer();
        mst.run();
    }
}