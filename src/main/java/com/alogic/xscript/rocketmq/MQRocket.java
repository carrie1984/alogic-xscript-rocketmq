package com.alogic.xscript.rocketmq;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.alogic.xscript.rocketmq.admin.RMQAdminConn;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class MQRocket extends Segment {
	protected String cid = "$mq-rocket";
	protected String server = "${server}";

	public MQRocket(String tag, Logiclet p) {
		super(tag, p);
		registerModule("mq-sender", RMQSender.class);
		registerModule("mq-puller", RMQPuller.class);
		registerModule("mq-pusher", RMQPusher.class);
		registerModule("mq-admin", RMQAdminConn.class);
	}

	@Override
	public void configure(Properties p) {
		super.configure(p);

		cid = PropertiesConstants.getString(p, "cid", cid, true);
		server = PropertiesConstants.getString(p, "server", server, true);
	}

	@Override
	protected void onExecute(Map<String, Object> root, Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {

		super.onExecute(root, current, ctx, watcher);

	}

}
