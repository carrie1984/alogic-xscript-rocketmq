package com.alogic.xscript.rocketmq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.alogic.xscript.rocketmq.util.ConsumerConnector;
import com.anysoft.util.BaseException;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;
import com.jayway.jsonpath.spi.JsonProvider;
import com.jayway.jsonpath.spi.JsonProviderFactory;
/**
 * 消费者接收消息
 * @author weibj
 *
 */
public class RMQReceive extends Segment implements MessageListenerConcurrently{
	/**
	 * producer的cid
	 */
	private String pid = "$consumer-conn";
	protected int size = 1;
	protected String tag = "data";
	protected boolean ignoreException=false;
	
	protected Map<String, Object> rootPara;
	protected Map<String, Object> currentPara;
	protected LogicletContext ctxPara;
	protected ExecuteWatcher watcherPara;
	
	/**
	 * 返回结果的id
	 */
	protected String id;
	
	public RMQReceive(String tag, Logiclet p) {
		super(tag, p);
	}
	
	public void configure(Properties p){
		super.configure(p);
		pid = PropertiesConstants.getString(p,"pid", pid);
		id = PropertiesConstants.getString(p,"id", "$" + getXmlTag());
		size = PropertiesConstants.getInt(p, "size",
				size);
		tag = PropertiesConstants.getRaw(p, "tag", tag);
		ignoreException = PropertiesConstants.getBoolean(p, "ignoreException", true);
	}

	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		ConsumerConnector conn = ctx.getObject(pid);
		rootPara = root;
		currentPara = current;
		ctxPara = ctx;
		watcherPara = watcher;
		
		String type = conn.getCurrentType();
		if(type.equals("push"))
		{
			receivePushMsg(conn,ignoreException);
		}
		
		if(type.equals("pull"))
		{
			List<String> result = new ArrayList<String>();
			result = conn.receivePullMsg(size, ignoreException);
			root.put(tag, result);
			if (result.size() > 0){
				for (String value:result){
					ctx.SetValue(id, value);
					super.onExecute(root, current, ctx, watcher);
				}
			}
		}
	}
	
	protected void receivePushMsg(ConsumerConnector conn, boolean ignoreException) {
		if (!conn.isConnected()) {
			if (!ignoreException)
				throw new BaseException("consumer.noconn", "the consumer is not connected.");
			logger.error("the consumer is not connected.");
		}
		
		DefaultMQPushConsumer consumer = conn.getPushConsumer();

		try {
			consumer.setConsumeMessageBatchMaxSize(size);
			consumer.registerMessageListener(this);
			
			consumer.start();
		} catch (MQClientException e) {
			if (!ignoreException)
        		throw new BaseException("rmq.ClientException",e.getMessage(),e);
		}
	}
	
	/**
	 * 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
	 */
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		synchronized (this) {
			for (MessageExt msg : msgs) {
				byte[] bytes = msg.getBody();
				ctxPara.SetValue(id, new String(bytes));
				//System.out.println(new String(bytes));
				super.onExecute(rootPara, currentPara, ctxPara, watcherPara);
				//JsonProvider provider = JsonProviderFactory.createProvider();
				//System.out.println(provider.toJson(rootPara));				
			}
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;	
		}
		
	}
}
