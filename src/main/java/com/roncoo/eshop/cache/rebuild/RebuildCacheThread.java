package com.roncoo.eshop.cache.rebuild;

import com.roncoo.eshop.cache.model.ProductInfo;
import com.roncoo.eshop.cache.service.CacheService;
import com.roncoo.eshop.cache.spring.SpringContext;
import com.roncoo.eshop.cache.zk.ZookeeperSession;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RebuildCacheThread implements Runnable{
    @Override
    public void run() {
        RebuildCacheQueue rebuildCacheQueue = RebuildCacheQueue.getInstance();
        ZookeeperSession session = ZookeeperSession.getInstance();
        CacheService cacheService = (CacheService) SpringContext.
                getApplicationContext().getBean("cacheService");
        while (true){
            try {

                ProductInfo productInfo = rebuildCacheQueue.takeProduction();
                Long productId = productInfo.getId();
                session.acquireDistributeLock(productId);
                try {
                    ProductInfo nowProduct = cacheService.getProductInfoFromReidsCache(productId);
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = simpleDateFormat.parse(productInfo.getModifiedTime());
                    Date existDate = simpleDateFormat.parse(nowProduct.getModifiedTime());
                    if(date.before(existDate)){
                        System.out.println("product time before existDate,product time:"+ productInfo.getModifiedTime()
                                +";existDate:"+ nowProduct.getModifiedTime());
                        cacheService.saveProductInfo2LocalCache(nowProduct);
                        continue;
                    }
                    cacheService.saveProductInfo2ReidsCache(productInfo);
                }finally {
                    session.releaseDistributeLock(productInfo.getId());
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
