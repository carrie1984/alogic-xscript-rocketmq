package com.alogic.xscript.rocketmq;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.alogic.xscript.plugins.Sleep;
import com.alogic.xscript.rocketmq.util.ConsumerConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class RMQPusher extends Segment{
	
	/**
	 *消费类型， type为push，则采用PushConsumer进行消息消费，应用向Consumer
	 *注册一个Listener监听器，Consumer收到消息立刻回调Listener监听器；
	 */
	protected String type = "push";
	
	protected String cid = "$mq-pusher";
	protected String server = "${server}";
	protected String consumerGroup = "${consumer.group}";
	protected String topic = "default";
	protected String tag = "default";
	
	public RMQPusher(String tag, Logiclet p) {
		super(tag, p);	
		registerModule("mq-push",RMQPush.class);
		registerModule("mq-wait",Sleep.class);
	}

	@Override
	public void configure(Properties p){
		super.configure(p);
		
		cid = PropertiesConstants.getString(p,"cid",cid,true);
		server = PropertiesConstants.getString(p,"server",server,true);
		topic = PropertiesConstants.getString(p,"topic",topic,true);
		consumerGroup = PropertiesConstants.getString(p,"consumerGroup",consumerGroup,true);
		tag = PropertiesConstants.getString(p,"tags",tag,true);
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
