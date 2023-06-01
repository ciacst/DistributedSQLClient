package org.example.client;

import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.example.Raft.Client;
import org.example.api.GetRegionServerResp;
import org.example.api.MasterClientService;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class client {
    static String Master_IpAddress="zookeeper://127.0.0.1:2181";
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

    public static String getTableOfQuerySql(String sql, String special) {
        String[] tokens = sql.split(" ");
        for(int i = 0; i < tokens.length; i++) {
            if(tokens[i].toLowerCase().equals(special) && i + 1 < tokens.length)
                return tokens[i+1];
        }
        return "";
    }
    public static Map<String, GetRegionServerResp> getMap() {
        Map<String,GetRegionServerResp> result = new HashMap<>();
        try {
            File f = new File("map.ser");
            if (!f.exists()) {
                String filePath = "map.ser";
                File file = new File(filePath);
                try {
                    file.createNewFile();
                    System.out.println("文件创建成功");
                    return result;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            FileInputStream fileInputStream = new FileInputStream("map.ser");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            result = (Map<String, GetRegionServerResp>)objectInputStream.readObject();
            fileInputStream.close();
            objectInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return result;
    }
    public static GetRegionServerResp FindExistTable(String SQL) {
        String head = SQL.split(" ")[0].toLowerCase();
        String table_name = "";
        if (head.equals("insert")) {
            table_name = getTableOfQuerySql(SQL, "into");
        }
        else if (head.equals("update")) {
            table_name = getTableOfQuerySql(SQL, "update");
        }
        else {
            table_name = getTableOfQuerySql(SQL, "from");
        }
        Map<String, GetRegionServerResp> my_map = getMap();
        if (my_map.isEmpty()) {
            return null;
        }
        else if (my_map.containsKey(table_name)) {
            String region_tmp = my_map.get(table_name).Region;
            String peers_tmp = my_map.get(table_name).Peers;
            GetRegionServerResp tmp = new GetRegionServerResp(region_tmp, peers_tmp, true);
            System.out.println("The table is in cache! Trying to load region server information...");
            return tmp;
        }
        else {
            return null;
        }
    }

    public static void AddTable(GetRegionServerResp new_region, String table_name) {
        try {
            Map<String, GetRegionServerResp> my_map = getMap();
            my_map.put(table_name, new_region);
            FileOutputStream fileOutputStream = new FileOutputStream("map.ser");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(my_map);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void DeleteTable(String table_name) {
        try {
            Map<String, GetRegionServerResp> my_map = getMap();
            my_map.remove(table_name);
            FileOutputStream fileOutputStream = new FileOutputStream("map.ser");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(my_map);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() throws IOException {
        // 连master
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
        String raftGroupId = "";
        String peers = "";

        while(true){
            line=reader.readLine();
            String head = line.split(" ")[0].toLowerCase();
            String table_name = "";
            if(line.toLowerCase().equals("quit")){
                GoodBye();
                break;
            }
            else if(line.toLowerCase().equals("tables")) {
                System.out.println(service.GetAllTables());
                continue;
            }
            else if(line.toLowerCase().equals("regions")) {
                System.out.println(service.GetRegionPeers());
                continue;
            }
            else if (head.equals("drop") || head.equals("create")) {
                GetRegionServerResp resp = service.GetRegionServer(line);
                if(!resp.Found) {
                    System.out.println("Table not find or sql invalid.");
                    continue;
                }
                raftGroupId = resp.Region;
                peers = resp.Peers;
                System.out.println(raftGroupId + " " + peers);
                if (head.equals("drop")) {
                    table_name = getTableOfQuerySql(line, "table");
                    DeleteTable(table_name);
                }
                else {
                    table_name = getTableOfQuerySql(line, "table");
                    AddTable(resp, table_name);
                }
            }
            else {
                GetRegionServerResp resp = FindExistTable(line);
                table_name = "";
                if (head.equals("insert")) {
                    table_name = getTableOfQuerySql(line, "into");
                }
                else if (head.equals("update")) {
                    table_name = getTableOfQuerySql(line, "update");
                }
                else {
                    table_name = getTableOfQuerySql(line, "from");
                }
                if (resp == null) {
                    System.out.println("Can't find the table. Trying to ask master and add it to cache...");
                    resp = service.GetRegionServer(line);
                    if(!resp.Found) {
                        System.out.println("Table not find or sql invalid.");
                        continue;
                    }
                    raftGroupId = resp.Region;
                    peers = resp.Peers;
                    System.out.println(raftGroupId + " " + peers);
                    AddTable(resp, table_name);
                }
//                 using gRPC to connect the RegionServer
            }

            try {
                System.out.println("raftGroupId:" + raftGroupId);
                System.out.println("peers:" + peers);
                Client test = new Client(raftGroupId, peers);

                test.run();
                PrintTable(test.operation(line));

            }
            catch(AlreadyClosedException e) {
                DeleteTable(table_name);

                GetRegionServerResp resp = service.GetRegionServer(line);
                if(!resp.Found) {
                    System.out.println("Table not find or sql invalid.");
                    continue;
                }
                raftGroupId = resp.Region;
                peers = resp.Peers;

                GetRegionServerResp tmp_resp = new GetRegionServerResp(raftGroupId, peers);
                AddTable(tmp_resp, table_name);

                Client test = new Client(raftGroupId, peers);

                test.run();
                PrintTable(test.operation(line));
            }
            catch(Exception e) {
                e.printStackTrace();
            }

        }

    }
}
