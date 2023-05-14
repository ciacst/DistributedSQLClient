package org.example;

import org.example.Raft.Client;
import org.example.Raft.Server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        // For both server and client
        String raftGroupId;
        String peers;
        // For server only
        String id;
        File storageDir;

        Properties props = new Properties();
        try {
            // 从文件中读取配置信息
            FileInputStream fis = new FileInputStream("config.properties");
            props.load(fis);
            fis.close();

            // 获取属性值
            raftGroupId = props.getProperty("raftGroupId");
            peers = props.getProperty("peers");
            id = props.getProperty("id");
            storageDir = new File(props.getProperty("storageDir"));

            // 输出属性值
            System.out.println("raftGroupId: " + raftGroupId);
            System.out.println("peers: " + peers);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

//        Server core = new Server(raftGroupId,peers,id,storageDir);
        Client test = new Client(raftGroupId,peers);

        try {
//            core.run();
            test.run();
            test.operation("show tables");
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}