package org.example.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Watcher {
//    static CuratorFramework zkClient;


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
                System.out.println("现在有 " + zkClient.getChildren().forPath(path).size() + " 个节点");
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
