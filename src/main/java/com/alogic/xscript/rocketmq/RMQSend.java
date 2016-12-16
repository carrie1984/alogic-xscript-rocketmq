package com.alogic.xscript.rocketmq;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.rocketmq.util.ProduerConnector;
import com.anysoft.util.BaseException;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * 生产者发送消息
 * @author weibj
 *
 */
public class RMQSend extends AbstractLogiclet{
	/**
	 * producer的cid
	 */
	private String pid = "$mq-sender";
	
	protected String topic = "test";
	protected String tags = "default";
	protected String key = "default";
	protected String data = "test";
	protected boolean ignoreException=false;
	
	/**
	 * 返回结果的id
	 */
	protected String id;
	
	public RMQSend(String tag, Logiclet p) {
		super(tag, p);
	}
	
	public void configure(Properties p){
		super.configure(p);
		pid = PropertiesConstants.getString(p,"pid", pid);
		id = PropertiesConstants.getString(p,"id", "$" + getXmlTag());
		topic = PropertiesConstants.getString(p, "topic", "");
		tags = PropertiesConstants.getString(p, "tags", "");
		key = PropertiesConstants.getString(p, "key", "");
		data = PropertiesConstants.getString(p, "data", "");
		ignoreException = PropertiesConstants.getBoolean(p, "ignoreException", ignoreException);
	}

	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		ProduerConnector producer = ctx.getObject(pid);
		producer.sendMsg(topic, tags, key, data, ignoreException);
	}

}
