package com.roncoo.eshop.cache.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class ZookeeperSession {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private ZooKeeper zookeeper;

    private ZookeeperSession()  {
        try {
            //去连接zookeeper server创建会话的时候是异步去执行的，
            //所以需要给一个监听器，告诉我们什么时候才是真正完成了跟zookeeper的连接
            zookeeper = new ZooKeeper("127.0.0.1:2181,192.0.0.1:2182,127.0.0.1:2183"
                    ,50000,new ZookeeperWatcher());
            //给一个状态 连接中
            System.out.println(zookeeper.getState());
            connectedSemaphore.await();
            System.out.println("Zookeeper session established........");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 加锁
     * @param productId
     */
    public void acquireDistributeLock(Long productId){
        String path = "/product-lock-"+ productId;
        try {
            zookeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("success to acquire lock for product[Id="+productId+"]");
        }catch (Exception e){
            //如果商品id对应的锁存在就会报 NodeExistException
            int count =0;
            while (true){
                try {
                    Thread.sleep(200);
                    zookeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }catch (Exception e1){
                    e1.printStackTrace();
                    count++;
                    continue;
                }
                System.out.println("success to acquire lock for product[Id="+productId+"] after try "+ count+"times");
                break;
            }
        }
    }

    /**
     * 加锁
     * @param path
     */
    public void acquireDistributeLock( String path){

        try {
            zookeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("success to acquire lock for path"+path);
        }catch (Exception e){
            //如果商品id对应的锁存在就会报 NodeExistException
            int count =0;
            while (true){
                try {
                    Thread.sleep(200);
                    zookeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }catch (Exception e1){
                    e1.printStackTrace();
                    count++;
                    continue;
                }
                System.out.println("success to acquire lock for path="+path+" after try "+ count+"times");
                break;
            }
        }
    }


    public void createNodeData(String path) {
        try {
            zookeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 加锁
     * @param path
     */
    public boolean acquireFastFailDistributeLock( String path){

        try {
            zookeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("success to acquire lock for"+path);
            return true;
        }catch (Exception e){
            System.out.println("false to acquire lock for"+path);
        }
        return false;
    }

    /**
     * 释放锁
     * @param productId
     */
    public void releaseDistributeLock(Long productId){
        String path = "/product-lock-"+ productId;
        try {
            zookeeper.delete(path,-1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放锁
     * @param path
     */
    public void releaseDistributeLock( String path){
        try {
            zookeeper.delete(path,-1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getNodeData(String path) {
        try {
            return new String(zookeeper.getData(path, false, new Stat()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public void setNodeData(String path, String data) {
        try {
            zookeeper.setData(path, data.getBytes(), -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取单列
     * @return
     */
    public static ZookeeperSession getInstance(){
        return Singleton.getInstance();
    }

    public static void  init(){
        getInstance();
    }


    private static class Singleton{
        private static ZookeeperSession instance;
        static {
            instance = new ZookeeperSession();
        }

        public static ZookeeperSession getInstance(){
            return instance;
        }
    }

    /**
     * zookeeper监听器
     */
    private class ZookeeperWatcher implements Watcher{

        @Override
        public void process(WatchedEvent watchedEvent) {
            System.out.println("Receive watched event:" + watchedEvent);
            if(Event.KeeperState.SyncConnected == watchedEvent.getState()){
                connectedSemaphore.countDown();
            }
        }
    }

    public static void main(String[] args) {
        ZookeeperSession.init();
    }
}
