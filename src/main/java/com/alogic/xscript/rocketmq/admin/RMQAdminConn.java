package com.alogic.xscript.rocketmq.admin;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.alogic.xscript.rocketmq.util.MQAdminConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * RocketMQ的管理者
 * @author weibj
 *
 */
public class RMQAdminConn extends Segment{
	//元素id
	protected String cid = "$admin-conn";
	
	//服务连接地址
	protected String server = "${server}";
	
	public RMQAdminConn(String tag, Logiclet p) {
		super(tag, p);	
		registerModule("create-topic",RMQUpdateTopic.class);
		registerModule("alter-topic",RMQUpdateTopic.class);
		registerModule("list-topic",RMQListTopic.class);
		registerModule("del-topic",RMQDelTopic.class);
		registerModule("desc-topic",RMQDescTopic.class);
	}

	@Override
	public void configure(Properties p){
		super.configure(p);
		
		cid = PropertiesConstants.getString(p,"cid",cid,true);
		server = PropertiesConstants.getString(p,"server",server,true);
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx, ExecuteWatcher watcher) {
		MQAdminConnector conn = new MQAdminConnector(ctx,server);
		try {
			ctx.setObject(cid, conn);
			super.onExecute(root, current, ctx, watcher);
		}finally{
			ctx.removeObject(cid);
			conn.disconnect();
		}
	}

}
