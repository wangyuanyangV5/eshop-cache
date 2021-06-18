package com.roncoo.eshop.cache.rebuild;


import com.roncoo.eshop.cache.model.ProductInfo;

import java.util.concurrent.ArrayBlockingQueue;

public class RebuildCacheQueue {
    private RebuildCacheQueue(){

    }

    private ArrayBlockingQueue<ProductInfo> queue = new ArrayBlockingQueue(1000);

    private static class Singleton{
        private static RebuildCacheQueue instance;
        static {
            instance = new RebuildCacheQueue();
        }

        public static RebuildCacheQueue getInstance(){
            return instance;
        }
    }

    public void add(ProductInfo productInfo){
        try {
            queue.put(productInfo);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ProductInfo takeProduction(){
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取单列
     * @return
     */
    public static RebuildCacheQueue getInstance(){
        return RebuildCacheQueue.Singleton.getInstance();
    }

    public static void  init(){
        getInstance();
    }
}
