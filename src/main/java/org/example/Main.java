package org.example;

import org.apache.dubbo.config.ReferenceConfig;
import org.example.Raft.Client;

import java.io.*;
import java.util.Properties;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;

import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.api.GetRegionServerResp;
import org.example.api.MasterClientService;



public class Main {
    static String Master_IpAddress="zookeeper://172.24.193.26:2181";
    static String Application_Name="client-service-caller";



    public static void Welcome(){
        System.out.println("Welcome !");
    }
    public static void GoodBye(){
        System.out.println("GoodBye !");
    }
    public static void PrintTable(String result){
        System.out.println(result);
    }

    public static void main(String[] args) throws IOException {
//        // 连master
        ReferenceConfig<MasterClientService> reference = new ReferenceConfig<>();
        reference.setInterface(MasterClientService.class);

        DubboBootstrap.getInstance()
                .application(Application_Name)
                .registry(new RegistryConfig(Master_IpAddress))
                .reference(reference);
        //Input From the User-----
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");

        Welcome();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line;

        MasterClientService service = reference.get();

        while(true){
            line=reader.readLine();

            if(line.toLowerCase().equals("quit")){
                GoodBye();
                break;
            }
            else if(line.toLowerCase().equals("tables")) {
                System.out.println(service.GetAllTables());
            }
            else if(line.toLowerCase().equals("regions")) {
                System.out.println(service.GetRegionPeers());
            }
            else{
//                 using gRPC to connect the RegionServer
                GetRegionServerResp resp = service.GetRegionServer(line);
                if(!resp.Found) {
                    System.out.println("Table not find or sql invalid.");
                    continue;
                }
                String raftGroupId = resp.Region;
                String peers = resp.Peers;
                System.out.println(raftGroupId + " " + peers);




//                try {
//
//                   Client test = new Client(raftGroupId,peers);
//
//                    test.run();
//                    PrintTable(test.operation(line));
//
//                }catch(Exception e){
//                    e.printStackTrace();
//                }

            }

        }

    }
}