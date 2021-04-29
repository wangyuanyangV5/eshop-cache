package com.roncoo.eshop.cache.controller;

import com.roncoo.eshop.cache.model.ProductInfo;
import com.roncoo.eshop.cache.model.ShopInfo;
import com.roncoo.eshop.cache.service.CacheService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
public class CacheTestController {

  @Resource
  private CacheService cacheService;

  @RequestMapping("/testPutCache")
  public void testPutCache(ProductInfo productInfo) {
    System.out.println(productInfo.getId() + ":" + productInfo.getName());
    cacheService.saveLocalCache(productInfo);
  }

  @RequestMapping("/testGetCache")
  public ProductInfo testGetCache(Long id) {
    ProductInfo productInfo = cacheService.getLocalCache(id);
    System.out.println(productInfo.getId() + ":" + productInfo.getName());
    return productInfo;
  }

  @RequestMapping("/getProductInfo")
  public ProductInfo getProductInfo(Long productId){
       ProductInfo productInfo = null;
       //先从redis种获取数据
       productInfo = cacheService.getProductInfoFromReidsCache(productId);
       System.out.println("从redis缓存中获取商品信息缓存"+ productInfo);
       if(productInfo == null){
         productInfo = cacheService.getProductInfoFromLocalCache(productId);
         System.out.println("从cache缓存中获取商品信息缓存"+ productInfo);
       }

       if(productInfo == null){
         //需要重数据源拉取缓存,重建缓存
         System.out.println("从数据库中获取商品信息缓存"+ productInfo);
       }
       return productInfo;
  }

  @RequestMapping("/getProductInfo")
  public ShopInfo getShopInfo(Long shopId){
    ShopInfo shopInfo = null;
    //先从redis种获取数据
    shopInfo = cacheService.getShopInfoFromReidsCache(shopId);
    System.out.println("从redis缓存中获取商品信息缓存" + shopInfo);
    if(shopInfo == null){
      shopInfo = cacheService.getShopInfoFromLocalCache(shopId);
      System.out.println("从cache缓存中获取商品信息缓存"+ shopInfo);
    }

    if(shopInfo == null){
      System.out.println("从数据库中获取商品信息缓存"+ shopInfo);
      //需要重数据源拉取缓存,重建缓存
    }
    return shopInfo;
  }

}
