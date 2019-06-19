package com.xiaoji.duan.ann;

import org.apache.commons.lang3.StringUtils;

import cn.jiguang.common.ClientConfig;
import cn.jpush.api.JPushClient;
import cn.jpush.api.device.TagAliasResult;
import cn.jpush.api.push.PushResult;
import cn.jpush.api.push.model.Platform;
import cn.jpush.api.push.model.PushPayload;
import cn.jpush.api.push.model.audience.Audience;
import cn.jpush.api.push.model.audience.AudienceTarget;
import cn.jpush.api.push.model.notification.Notification;
import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.client.WebClient;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

// 综合通知服务
public class MainVerticle extends AbstractVerticle {

	private WebClient client = null;
	private AmqpBridge bridge = null;
	private MongoClient mongodb = null;
	private RabbitMQClient rabbitmq = null;
	private JPushClient jpushClient = null;

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		client = WebClient.create(vertx);

		JsonObject config = new JsonObject();
		config.put("host", "mongodb");
		config.put("port", 27017);
		config.put("keepAlive", true);
		mongodb = MongoClient.createShared(vertx, config);

		bridge = AmqpBridge.create(vertx);

		bridge.endHandler(handler -> {
			connectStompServer();
		});

		connectStompServer();

		RabbitMQOptions rmqconfig = new RabbitMQOptions(config().getJsonObject("rabbitmq"));

		rabbitmq = RabbitMQClient.create(vertx, rmqconfig);
		
		rabbitmq.start(handler -> {
			if (handler.succeeded()) {
				System.out.println("rabbitmq connected.");
			} else {
				System.out.println("rabbitmq connect failed with " + handler.cause().getMessage());
			}
		});
		
		ClientConfig jpushconfig = ClientConfig.getInstance();
		jpushconfig.setMaxRetryTimes(config().getJsonObject("jpush", new JsonObject()).getInteger("maxretry", 5));
		jpushconfig.setConnectionTimeout(config().getJsonObject("jpush", new JsonObject()).getInteger("timeout", 10000));	// 10 seconds
		jpushconfig.setSSLVersion(config().getJsonObject("jpush", new JsonObject()).getString("sslversion", "TLSv1.1"));		// JPush server supports SSLv3, TLSv1, TLSv1.1, TLSv1.2
		
		jpushClient = new JPushClient(
				config().getJsonObject("jpush", new JsonObject()).getString("secret", ""),
				config().getJsonObject("jpush", new JsonObject()).getString("appkey", ""),
				null,
				jpushconfig);
	}

	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"),
				config().getInteger("stomp.server.port", 5672), res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						connectStompServer();
					} else {
						subscribeTrigger(config().getString("amq.app.id", "ann"));
					}
				});
	}
	
	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}
	
	public static String getShortContent(String origin) {
		return origin.length() > 512 ? origin.substring(0, 512) : origin;
	}
	
	private PushPayload buildPushObject_android(TagAliasResult tagalias) {
        return PushPayload.newBuilder()
        		.setPlatform(Platform.android())
        		.setAudience(Audience.newBuilder()
                        .addAudienceTarget(AudienceTarget.tag(tagalias.tags))
                        .addAudienceTarget(AudienceTarget.alias(tagalias.alias))
                        .build())
        		.setMessage(cn.jpush.api.push.model.Message.newBuilder()
                        .setMsgContent("Test from API Example - msgContent")
                        .addExtra("from", "JPush")
                        .build())
        		.setNotification(Notification.alert("Test for Alert"))
        		.build();
	}
	
	/**
	 * 
	 * 冥王星综合通知服务
	 * 
	 * 通知类型
	 * 	日程: agenda_from_share, agenda_from_share_modify, agenda_from_share_remove
	 *  语音: inteligence_create_agenda, inteligence_search_agenda, inteligence_mix ...
	 *  通知: mwxing_message
	 *  提醒: mwxing_announce
	 * 
	 * 日程共享通知被共享人
	 * 已共享日程修改/删除通知被共享人
	 * 已共享日程删除共享人员,通知被删除人员
	 * 已共享日程添加共享人员,通知新增共享人员
	 * 
	 * 语音结果通知语音发起人所使用的设备
	 * 
	 * 系统通知
	 * 系统提醒
	 * 
	 * @param consumer
	 * @param received
	 */
	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + getShortContent(received.body().encode()) + "]");
		JsonObject data = received.body().getJsonObject("body");

		Boolean cachestore = Boolean.valueOf(data.getJsonObject("context").getString("cache", "true"));
		
		JsonArray announceTo = new JsonArray();
		
		if (data.getJsonObject("context").getValue("announceTo") != null) {
			if (data.getJsonObject("context").getValue("announceTo", new JsonArray()) instanceof JsonArray) {
				announceTo.addAll(data.getJsonObject("context").getJsonArray("announceTo", new JsonArray()));
			} else {
				announceTo.add(data.getJsonObject("context").getValue("announceTo", new JsonObject()));
			}
		}

		if (announceTo == null || announceTo.isEmpty()) {
			System.out.println("No announce target, process stopped.");
			return;
		} else {
			System.out.println("Announce target exist, process next.");
		}
		
		String announceType = data.getJsonObject("context").getString("announceType", "");

		JsonObject announceContent = new JsonObject();
		if (data.getJsonObject("context").getValue("announceContent") != null) {
			System.out.println(data.getJsonObject("context").getValue("announceContent").getClass().getName());
		}
		if (data.getJsonObject("context").getValue("announceContent", new JsonObject()) instanceof JsonObject) {
			announceContent.mergeIn(data.getJsonObject("context").getJsonObject("announceContent", new JsonObject()));
		} else {
			announceContent.mergeIn(new JsonObject().put("data", data.getJsonObject("context").getValue("announceContent", new JsonObject())));
		}
		String next = data.getJsonObject("context").getString("next");

		if ("duan_announce".equals(announceType)) {
			// 短应用内部通知
			for (int pos = 0; pos < announceTo.size(); pos++) {
				String address = announceTo.getString(pos);
				System.out.println("Announced to " + address + " " + getShortContent(announceContent.encode()));

				MessageProducer<JsonObject> producer = bridge.createProducer(address);
				producer.send(new JsonObject()
						.put("body", new JsonObject()
								.put("context", announceContent)));
			}
			
		} else if ("agenda_from_share".equals(announceType)) {
			for (int pos = 0; pos < announceTo.size(); pos++) {
				String openid = announceTo.getString(pos);
				System.out.println("Announce to " + openid + " start process.");
				Future<JsonObject> future = Future.future();
				
				future.setHandler(handler -> {
					if (handler.succeeded()) {
						JsonObject userinfo = handler.result();
						
						System.out.println("User info fetched with " + openid);
						System.out.println(getShortContent(userinfo.encode()));
						String unionId = userinfo.getJsonObject("data").getString("unionid");
						String openId = userinfo.getJsonObject("data").getString("openid");

						if (openId == null || StringUtils.isEmpty(openId)) {
							//账户不存在通过短消息提醒
							System.out.println("announce by sms to " + openid);
							
							if (cachestore) {
								// 缓存未注册用户数据, 用户注册登录后通知
								JsonObject storage = new JsonObject();
								storage.put("openid", openid);
								storage.put("announceTo", new JsonArray().add(openid));
								storage.put("announceType", announceType);
								storage.put("announceContent", announceContent);
								
								MessageProducer<JsonObject> producer = bridge.createProducer("aak");
								producer.send(new JsonObject().put("body", storage));
							}
							
							// 发送短信通知
							JsonObject sms = announceContent.getJsonObject("sms");
							
							sms.put("templateid", sms.getJsonObject("template").getString("newuser"));
							
							sendShortMessages(openid, sms);

						} else {
							//账户已存在通过冥王星消息队列推送
							String routingkey = "mwxing." + unionId;
							System.out.println("announce by mwxing message to " + routingkey);
							sendMQMessages(config().getString("exchange.mwxing.direct", "exchange.mwxing.direct"), routingkey, announceContent.getJsonObject("mwxing"));
							
							//同时通过极光推送
							if (userinfo.containsKey("device")) {
								JsonObject device = userinfo.getJsonObject("device", new JsonObject());
								
						        System.out.println("JPush consider with " + device.encode());
						        
								String jpushId = device.getJsonObject("jpush", new JsonObject()).getString("id", "");

								// 如果Jpush Id不存在则不推送
								if (!StringUtils.isEmpty(jpushId)) {
									try {
										//获取别名
								        TagAliasResult aliasresult = jpushClient.getDeviceTagAlias(jpushId);
								        
								        PushPayload payload = buildPushObject_android(aliasresult);
								        
								        PushResult result = jpushClient.sendPush(payload);
								        System.out.println("JPush responsed with " + result);
									} catch(Exception e) {
										e.printStackTrace();
									}
								}
								
							} else {
						        System.out.println("JPush skipped without device info.");
							}

						}
						
					} else {
						System.out.println("User info fetched error with " + handler.cause().getMessage());
						System.out.println("announce by sms to " + openid);

						if (cachestore) {
							// 缓存未注册用户数据, 用户注册登录后通知
							JsonObject storage = new JsonObject();
							storage.put("openid", openid);
							storage.put("announceTo", new JsonArray().add(openid));
							storage.put("announceType", announceType);
							storage.put("announceContent", announceContent);
							
							MessageProducer<JsonObject> producer = bridge.createProducer("aak");
							producer.send(new JsonObject().put("body", storage));
						}

						// 发送短信通知
						JsonObject sms = announceContent.getJsonObject("sms");
						
						sms.put("templateid", sms.getJsonObject("template").getString("newuser"));
						sendShortMessages(openid, sms);
					}
				});
				
				getUserInfo(future, openid);
			}
		} else if ("inteligence_mix".equals(announceType)) {
			for (int pos = 0; pos < announceTo.size(); pos++) {
				StringBuffer openid = new StringBuffer(announceTo.getString(pos));
				StringBuffer deviceid = new StringBuffer();
				if (openid.indexOf(";") > 0) {
					String[] ids = openid.toString().split(";");
					
					openid.delete(0, openid.length());
					openid.append(ids[0]);
					deviceid.append(ids[1]);
				}
				System.out.println("Announce to " + openid + "[" + deviceid + "]" + " start process.");
				Future<JsonObject> future = Future.future();
				
				future.setHandler(handler -> {
					if (handler.succeeded()) {
						JsonObject userinfo = handler.result();
						
						System.out.println("User info fetched with " + openid);
						System.out.println(getShortContent(userinfo.encode()));
						String unionId = userinfo.getJsonObject("data").getString("unionid");
						String openId = userinfo.getJsonObject("data").getString("openid");
						
						if (openId == null || StringUtils.isEmpty(openId)) {
							System.out.println("inteligence message can not announce by sms to " + openid);
						} else {
							if ("".equals(deviceid)) {
								String routingkey = "mwxing.announce." + unionId;
								System.out.println("announce by mwxing message to " + routingkey);
								sendMQMessages(config().getString("exchange.mwxing.direct", "exchange.mwxing.direct"), routingkey, announceContent.getJsonObject("mwxing"));
							} else {
								String routingkey = "mwxing." + unionId + "." + deviceid;
								System.out.println("announce by mwxing message to " + routingkey);
								sendMQMessages(config().getString("exchange.mwxing.direct", "exchange.mwxing.direct"), routingkey, announceContent.getJsonObject("mwxing"));
							}
						}
						
					} else {
						System.out.println("User info fetched error with " + handler.cause().getMessage());
						System.out.println("inteligence message can not announce by sms to " + openid);
					}
				});
				
				getUserInfo(future, openid.toString());
			}
		} else {
			System.out.println("Received process undefined messages.");
		}
		
		JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("complete", new JsonObject()));
		
		MessageProducer<JsonObject> producer = bridge.createProducer(next);
		producer.send(new JsonObject().put("body", nextctx));
		System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + getShortContent(nextctx.encode()) + "]");

	}

	private void sendMQMessages(String exchange, String routingkey, JsonObject content) {
		rabbitmq.basicPublish(exchange, routingkey, new JsonObject().put("body", content.encode()), resultHandler -> {
			if (resultHandler.succeeded()) {
				System.out.println("Send rabbit mq message successed. [" + getShortContent(content.encode()) + "]");
			} else {
				System.out.println("Send rabbit mq message failed with " + resultHandler.cause().getMessage());
			}
		});
	}
	
	private void sendShortMessages(String phoneno, JsonObject content) {
		System.out.println("sms starting...");
		client.head(
				config().getInteger("sms.service.port", 8080),
				config().getString("sms.service.host", "sa-sms"),
				config().getString("sms.service.starter.singlesend", "/sms/send"))
		.method(HttpMethod.POST)
		.addQueryParam("platformType", "*")
		.addQueryParam("mobile", phoneno)
		.addQueryParam("sendType", content.getString("templateid"))
		.addQueryParam("sendContent", content.getString("content"))
		.send(handler -> {
				if (handler.succeeded()) {
					System.out.println("sms response " + handler.result().statusCode() + " " + handler.result().bodyAsString());
					System.out.println("sms sent announce message to " + phoneno + " completed. [" + content + "]");
				} else {
					handler.cause().printStackTrace();
					System.out.println("sms sent announce message to " + phoneno + " failed. [" + content + "]");
				}
			}
		);
		System.out.println("sms end.");
	}
	
	private void getUserInfo(Future<JsonObject> future, String phoneno) {
		
		client
		.head(config().getInteger("mwxing.auth.port", 8080), config().getString("mwxing.auth.host", "sa-aup"), "/aup/data/" + phoneno + "/userinfo")
		.method(HttpMethod.GET)
		.send(handler -> {
			if (handler.succeeded()) {
				JsonObject userinfo = null;
				
				try {
					userinfo = handler.result().bodyAsJsonObject();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (userinfo == null) {
						userinfo = new JsonObject();
					}
				}
				
				future.complete(userinfo);
			} else {
				handler.cause().printStackTrace();
				future.fail(handler.cause());
			}
		});
		
	}
}
