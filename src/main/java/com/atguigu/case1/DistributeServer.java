package com.atguigu.case1;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DistributeServer {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeServer server = new DistributeServer();
        server.getConnection();
        server.registServer(args[0]);
        server.business(args[0]);
    }

    private void business(String hostName) throws InterruptedException {
        System.out.println(hostName+"开始business...........");
        Thread.sleep(Long.MAX_VALUE);
    }

    private void registServer(String hostName) throws InterruptedException, KeeperException {
        zk.create("/servers/server",hostName.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("server注册成功...........");
    }

    private void getConnection() throws IOException {
        new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {


            }
        });
    }
    }
