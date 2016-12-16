package com.alogic.xscript.rocketmq;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.alogic.xscript.rocketmq.util.ConsumerConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class RMQPuller extends Segment{
	
	/**
	 * 消费者的类型为pull，则采用PullConsumer进行消息消费，应用主动调用Consumer
	 * 的拉取消息方法从Broker拉消息，主动权由应用控制 
	 */
	protected String type = "pull";
		
	protected String cid = "$mq-puller";
	protected String server = "${server}";
	protected String consumerGroup = "${consumer.group}";
	protected String topic = "default";
	protected String tag = "default";
	
	public RMQPuller(String tag, Logiclet p) {
		super(tag, p);	
		registerModule("mq-pull",RMQPull.class);
	}

	@Override
	public void configure(Properties p){
		super.configure(p);
		
		cid = PropertiesConstants.getString(p,"cid",cid,true);
		server = PropertiesConstants.getString(p,"server",server,true);
		topic = PropertiesConstants.getString(p,"topic",topic,true);
		consumerGroup = PropertiesConstants.getString(p,"consumerGroup",consumerGroup,true);
		tag = PropertiesConstants.getString(p,"tags",tag,true);
		type = PropertiesConstants.getString(p,"type",type,true);
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx, ExecuteWatcher watcher) {
		ConsumerConnector conn = new ConsumerConnector(ctx,server,topic,consumerGroup,tag,type);
		try {
			ctx.setObject(cid, conn);
			super.onExecute(root, current, ctx, watcher);
		}finally{
			ctx.removeObject(cid);
			conn.disconnect();
		}
	}
}
