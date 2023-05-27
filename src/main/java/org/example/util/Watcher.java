package org.example.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.example.region.RegionServer;

import java.io.*;
import java.util.Properties;

import static org.example.region.RegionServer.service;

public class Watcher {
//    static CuratorFramework zkClient;

    public static String getMySQLDump(String ip,String port,String userName,String password,String databaseName) throws Exception{
        String[] cmdArray = {"mysqldump.exe","-h" + ip,"-P" + port,"-u" + userName,
                "-p" + password,"--databases",databaseName,"--no-create-db"};

        Process process = Runtime.getRuntime().exec(cmdArray);

        InputStream inputStream = process.getInputStream(); // 标准输出流
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        String line;
        StringBuilder SQLDumpBuilder = new StringBuilder();
        while ((line = bufferedReader.readLine()) != null) {
            if(line.length()>=3 && line.substring(0,3).equals("USE")){
                // do nothing
            }else{
                SQLDumpBuilder.append(line).append("\n");
            }
        }

        return SQLDumpBuilder.toString();
    }

    public static void createWatcher(String path, CuratorFramework zkClient) throws Exception {
        PathChildrenCache pathCache = new PathChildrenCache(zkClient, path,true);
        //添加监听器
        PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                ChildData childData = pathChildrenCacheEvent.getData();
                switch (pathChildrenCacheEvent.getType()) {
                    case CHILD_ADDED:
                        System.out.println("子节点增加，path=" + childData.getPath() + ",data=" + new String(childData.getData()));
                        break;
                    case CHILD_UPDATED:
                        System.out.println("子节点更新，path=" + childData.getPath() + ",data=" + new String(childData.getData()));
                        break;
                    case CHILD_REMOVED:
                        System.out.println("子节点删除，path=" + childData.getPath() + ",data=" + new String(childData.getData()));
                        break;
                    default:
                        break;
                }
                int remainNodeSize = zkClient.getChildren().forPath(path).size();
                System.out.println("现在有 " + remainNodeSize + " 个节点");
                if(remainNodeSize == 1 && pathChildrenCacheEvent.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED){
                    System.out.println("开始分片迁移");
                    // 1. execute mysql dump
                    Properties props = new Properties();
                    try {
                        // 从文件中读取配置信息
                        FileInputStream fis = new FileInputStream("config.properties");
                        props.load(fis);
                        fis.close();

                        // 获取属性值
                        String ip = props.getProperty("ip");
                        String port = props.getProperty("port");
                        String userName = props.getProperty("userName");
                        String password = props.getProperty("password");
                        String databaseName = props.getProperty("databaseName");
                        String raftGroupId = props.getProperty("raftGroupId");

                        String mySQLDump = getMySQLDump(ip,port,userName,password,databaseName);
                        // 2. call
                        if(service.ReportFailure(raftGroupId,mySQLDump)){

                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        pathCache.getListenable().addListener(pathChildrenCacheListener);
        pathCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    }

//    public static void createZkClient(String ServerAddress) {
//        String zkServerAddress = ServerAddress;
//        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
//        zkClient = CuratorFrameworkFactory.builder()
//                .connectString(zkServerAddress)
//                .sessionTimeoutMs(5000)
//                .connectionTimeoutMs(5000)
//                .retryPolicy(retryPolicy)
//                .build();
//        zkClient.start();
//    }

}
