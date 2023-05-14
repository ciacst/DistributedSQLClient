package org.example;

import org.apache.dubbo.config.ReferenceConfig;
import org.example.Raft.Client;

import java.io.*;
import java.util.Properties;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;

import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.api.MasterClientService;



public class Main {
    static String Master_IpAddress="zookeeper://127.0.0.1:2181";
    static String Application_Name="client-service";



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
                MasterClientService service = reference.get();
                String raftGroupId = "demoRaftGroup123";
                String peers = service.GetRegionServer(line);





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