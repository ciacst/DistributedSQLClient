package org.example.master;

import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;

import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;


import org.example.api.*;
import org.example.util.TableRouter;

import java.util.ServiceConfigurationError;

public class MasterServer {
    private static final String ZOOKEEPER_HOST = System.getProperty("zookeeper.address", "172.24.196.71");
    private static final String ZOOKEEPER_PORT = System.getProperty("zookeeper.port", "2181");
    private static final String ZOOKEEPER_ADDRESS = "zookeeper://" + ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT;

    public void run() {
        System.out.println(ZOOKEEPER_ADDRESS);

        ServiceConfig<MasterClientService> clientService = new ServiceConfig<>();
        clientService.setInterface(MasterClientService.class);
        TableRouter TheRouter = new TableRouter("RegionStoreFile", "TableStoreFile");
        clientService.setRef(new MasterClientServiceImpl(TheRouter));
        ServiceConfig<MasterRegionService> regionService = new ServiceConfig<>();
        regionService.setInterface(MasterRegionService.class);
        regionService.setRef(new MasterRegionServiceImpl(TheRouter));
        DubboBootstrap.getInstance()
                .application("master-service")
                .registry(new RegistryConfig(ZOOKEEPER_ADDRESS))
                .protocol(new ProtocolConfig("dubbo", -1))
                .service(clientService)
                .service(regionService)
                .start()
                .await();
    }
}
