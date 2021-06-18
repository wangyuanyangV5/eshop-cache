package com.roncoo.eshop.cache.kafka;

import com.alibaba.fastjson.JSONObject;
import com.roncoo.eshop.cache.model.ProductInfo;
import com.roncoo.eshop.cache.model.ShopInfo;
import com.roncoo.eshop.cache.service.CacheService;
import com.roncoo.eshop.cache.spring.SpringContext;
import com.roncoo.eshop.cache.zk.ZookeeperSession;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/**
 * kafka消息处理线程
 * @author Administrator
 *
 */
@SuppressWarnings("rawtypes")
public class KafkaMessageProcessor implements Runnable {

	private KafkaStream kafkaStream;
	private CacheService cacheService;

	public KafkaMessageProcessor(KafkaStream kafkaStream) {
		this.kafkaStream = kafkaStream;
		this.cacheService = (CacheService) SpringContext.getApplicationContext()
				.getBean("cacheService");
	}

	@SuppressWarnings("unchecked")
	public void run() {
		ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        while (it.hasNext()) {
        	try {
				String message = new String(it.next().message());

				// 首先将message转换成json对象
				JSONObject messageJSONObject = JSONObject.parseObject(message);

				// 从这里提取出消息对应的服务的标识
				String serviceId = messageJSONObject.getString("serviceId");

				// 如果是商品信息服务
				if("productInfoService".equals(serviceId)) {
					processProductInfoChangeMessage(messageJSONObject);
				} else if("shopInfoService".equals(serviceId)) {
					processShopInfoChangeMessage(messageJSONObject);
				}
			}catch (Exception e){
        		e.printStackTrace();
			}
        }
	}

	/**
	 * 处理商品信息变更的消息
	 * @param messageJSONObject
	 */
	private void processProductInfoChangeMessage(JSONObject messageJSONObject) {
		// 提取出商品id
		Long productId = messageJSONObject.getLong("productId");

		// 调用商品信息服务的接口
		// 直接用注释模拟：getProductInfo?productId=1，传递过去
		// 商品信息服务，一般来说就会去查询数据库，去获取productId=1的商品信息，然后返回回来

		// 缓存架构：高并发、高性能、海量数据，等场景

		String productInfoJSON = "{\"id\": 2, \"name\": \"iphone7手机\", \"price\": 5599, \"pictureList\":\"a.jpg,b.jpg\", \"specification\": \"iphone7的规格\", \"service\": \"iphone7的售后服务\", \"color\": \"红色,白色,黑色\", \"size\": \"5.5\", \"shopId\": 1," +
				"\"modified_time\":\"2017-01-01 12:00:00\"}";
		ProductInfo productInfo = JSONObject.parseObject(productInfoJSON, ProductInfo.class);


		cacheService.saveProductInfo2LocalCache(productInfo);
		System.out.println("===================获取刚保存到本地缓存的商品信息：" + cacheService.getProductInfoFromLocalCache(productId));
		//将数据直接写入redis缓存之前应该获取一个zk分布式锁
		ZookeeperSession zkSession = ZookeeperSession.getInstance();
		zkSession.acquireDistributeLock(productId);
		try {
			//获取到了锁，先从redis获取数据
			ProductInfo nowProduct = cacheService.getProductInfoFromReidsCache(productId);
			if(Objects.nonNull(nowProduct)){
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				try {
					Date date = simpleDateFormat.parse(productInfo.getModifiedTime());
					Date existDate = simpleDateFormat.parse(nowProduct.getModifiedTime());
					if(date.before(existDate)){
						System.out.println("product time before existDate,product time:"+ productInfo.getModifiedTime()
								+";existDate:"+ nowProduct.getModifiedTime());
						cacheService.saveProductInfo2LocalCache(nowProduct);
						return;
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
				System.out.println("product time after existDate,product time:"+ productInfo.getModifiedTime()
						+";existDate:"+ nowProduct.getModifiedTime());
			}else {
				System.out.println("product not exit");
			}
			cacheService.saveProductInfo2ReidsCache(productInfo);
			//保持zookeeper锁
			try {
				Thread.sleep(200000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}finally {
			zkSession.releaseDistributeLock(productId);
		}
	}

	/**
	 * 处理店铺信息变更的消息
	 * @param messageJSONObject
	 */
	private void processShopInfoChangeMessage(JSONObject messageJSONObject) {
		// 提取出商品id
		Long productId = messageJSONObject.getLong("productId");
		Long shopId = messageJSONObject.getLong("shopId");

		// 调用商品信息服务的接口
		// 直接用注释模拟：getProductInfo?productId=1，传递过去
		// 商品信息服务，一般来说就会去查询数据库，去获取productId=1的商品信息，然后返回回来
		String shopInfoJSON = "{\"id\": 1, \"name\": \"小王的手机店\", \"level\": 5, \"goodCommentRate\":0.99}";
		ShopInfo shopInfo = JSONObject.parseObject(shopInfoJSON, ShopInfo.class);
		cacheService.saveShopInfo2LocalCache(shopInfo);
		System.out.println("===================获取刚保存到本地缓存的店铺信息：" + cacheService.getShopInfoFromLocalCache(shopId));
		cacheService.saveShopInfo2ReidsCache(shopInfo);
	}

}
