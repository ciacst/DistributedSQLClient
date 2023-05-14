package org.example;

import org.example.Raft.Client;

import java.io.*;
import java.util.Properties;

public class Main {
    String Master_IpAddress="zookeeper://127.0.0.1:2181";
    String Application_Name="first-dubbo-consumer";
    // Map<String,String> TableToAddress;

    // public boolean ExistCache(String tableName){
    //     boolean result=false;
    //     if(TableToAddress.get(tableName)){
    //         //Table exist in the Cache
    //         result=true;
    //     }
    //     return result;

    // }


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
//        ReferenceConfig<GreetingsService> reference = new ReferenceConfig<>();
//        reference.setInterface(MasterClientService.class);
//
//        DubboBootstrap.getInstance()
//                .application(Application_Name)
//                .registry(new RegistryConfig(Master_IpAddress))
//                .reference(reference);
        //Input From the User-----
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");

        Welcome();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line;
//        StringBuilder input = new StringBuilder();

        while(true){
            line=reader.readLine();

            if(line.equals("quit")){
                GoodBye();
                break;
            }
            else{
                //If the
                //Send SQL to Master Server Get the RegionServer Ip from the Master
//                MasterClientService service = reference.get();

//                String IndexAndIp_Address = service.GetRegionServer(line);
                // using gRPC to connect the RegionServer
                String raftGroupId = "demoRaftGroup123";
                String peers = "0:127.0.0.1:15100,1:127.0.0.1:15101,2:127.0.0.1:15102";


                try {
                    Client test = new Client(raftGroupId,peers);

                    test.run();
                    PrintTable(test.operation(line));

                }catch(Exception e){
                    e.printStackTrace();
                }

            }

        }

    }
}