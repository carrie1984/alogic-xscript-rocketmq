package com.alogic.xscript.rocketmq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.rocketmq.util.ConsumerConnector;
import com.anysoft.util.BaseException;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * 消费者接收消息
 * @author weibj
 *
 */
public class RMQReceive extends AbstractLogiclet{
	/**
	 * producer的cid
	 */
	private String pid = "$consumer-conn";
	protected int size = 1;
	protected String tag = "data";
	protected boolean ignoreException=false;
	
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
		ConsumerConnector consumer = ctx.getObject(pid);		
        root.put(ctx.transform(tag),consumer.receiveMsg(size, ignoreException));
	}
}
