package com.atguigu.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

    private final String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private final int sessionTimeout = 2000;
    private final ZooKeeper zk;
    private String waitPath;
    private String rootNode = "locks";
    private String subNode = "seq-";
    private String currentNode;

    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                }
                if (watchedEvent.getType() ==
                        Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath))
                {
                    waitLatch.countDown();
                }
            }
        });
        connectLatch.await();
        Stat stat = zk.exists("/" + rootNode, false);
        if(stat==null){
            zk.create("/"+rootNode,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
    }
    public void zklock() throws InterruptedException, KeeperException {
        currentNode = zk.create("/" + rootNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Thread.sleep(10);
        List<String> children = zk.getChildren("/" + rootNode, false);
        if(children.size()==1)return;
        else{
            Collections.sort(children);
            String thisNode = currentNode.substring(("/" +
                    rootNode + "/").length());
            int index = children.indexOf(thisNode);
            if(index==-1){

            }else if(index==0){
                return;
            }else{
                this.waitPath = "/" + rootNode + "/" +
                        children.get(index - 1);
                zk.getData(waitPath, true, new Stat());
                //进入等待锁状态
                waitLatch.await();
                return;
            }
        }
    }
    public void unZkLock() {
        try {
            zk.delete(this.currentNode, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

}