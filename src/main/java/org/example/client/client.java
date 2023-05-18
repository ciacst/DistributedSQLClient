package org.example.client;

import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.Raft.Client;
import org.example.api.GetRegionServerResp;
import org.example.api.MasterClientService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class client {
    static String Master_IpAddress="zookeeper://10.192.139.47:2181";
    static String Application_Name="client-service-caller";



    public void Welcome(){
        System.out.println("Welcome !");
    }
    public void GoodBye(){
        System.out.println("GoodBye !");
    }
    public void PrintTable(String result){
        System.out.println(result);
    }

    public void run() throws IOException {
//      // 连master
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