package org.example.master;

import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;

import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;


import org.example.api.*;

import java.util.ServiceConfigurationError;

public class MasterServer {
    private static final String ZOOKEEPER_HOST = System.getProperty("zookeeper.address", "172.22.104.243");
    private static final String ZOOKEEPER_PORT = System.getProperty("zookeeper.port", "2200");
    private static final String ZOOKEEPER_ADDRESS = "zookeeper://" + ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT;

    public void run() {
        System.out.println(ZOOKEEPER_ADDRESS);

        ServiceConfig<MasterClientService> clientService = new ServiceConfig<>();
        clientService.setInterface(MasterClientService.class);
        clientService.setRef(new MasterClientServiceImpl());
        DubboBootstrap.getInstance()
                .application("client-service-callee")
                .registry(new RegistryConfig(ZOOKEEPER_ADDRESS))
                .protocol(new ProtocolConfig("dubbo", -1))
                .service(clientService)
                .start()
                .await();
    }
}
