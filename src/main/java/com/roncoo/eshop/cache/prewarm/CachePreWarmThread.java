package com.roncoo.eshop.cache.prewarm;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.roncoo.eshop.cache.model.ProductInfo;
import com.roncoo.eshop.cache.service.CacheService;
import com.roncoo.eshop.cache.spring.SpringContext;
import com.roncoo.eshop.cache.zk.ZookeeperSession;

public class CachePreWarmThread extends Thread{
    private ZookeeperSession zkSession;

    @Override
    public void run() {
        ZookeeperSession zkSession = ZookeeperSession.getInstance();
        CacheService cacheService = (CacheService)SpringContext.getApplicationContext().getBean("cacheService");
        String taskList = zkSession.getNodeData("/taskid-list");
        if(taskList != null && !"".equals(taskList)){
            String[] taskIds = taskList.split(",");
            for(String taskId : taskIds){
                String taskIdLockPath = "/taskid-lock" + taskId;
                boolean result = zkSession.acquireFastFailDistributeLock(taskIdLockPath);
                if(!result){
                    continue;
                }
                String taskIdStatusLockPath = "/taskid-status-lock-"+ taskId;
                zkSession.acquireDistributeLock(taskIdStatusLockPath);

                String taskIdStatus = zkSession.getNodeData("taskid-status-"+ taskId);
                if(taskIdStatus == null || "".equals(taskIdStatus)){
                    String productIdList = zkSession.getNodeData("/task-hot-product-list-"+ taskId);
                    JSONArray jsonArray = JSONArray.parseArray(productIdList);
                    for(int i = 0;i < jsonArray.size();i++){
                        Long productId = jsonArray.getLong(i);
                        //需要重数据源拉取缓存,重建缓存
                        String productInfoJSON = "{\"id\": "+productId+", \"name\": \"iphone7手机\", \"price\": 5599, \"pictureList\":\"a.jpg,b.jpg\", \"specification\": \"iphone7的规格\", \"service\": \"iphone7的售后服务\", \"color\": \"红色,白色,黑色\", \"size\": \"5.5\", \"shopId\": 1," +
                                "\"modified_time\":\"2017-01-01 12:01:00\"}";
                        ProductInfo productInfo = JSONObject.parseObject(productInfoJSON,ProductInfo.class);
                        cacheService.saveProductInfo2LocalCache(productInfo);
                        cacheService.saveProductInfo2ReidsCache(productInfo);
                    }
                    zkSession.createNodeData("taskid-status-"+ taskId);
                    zkSession.setNodeData("taskid-status-"+ taskId,"success");
                }

                zkSession.releaseDistributeLock(taskIdStatusLockPath);
                zkSession.releaseDistributeLock(taskIdLockPath);
            }
        }
    }
}
