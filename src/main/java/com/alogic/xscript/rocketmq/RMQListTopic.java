package com.alogic.xscript.rocketmq;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.rocketmq.util.MQAdminConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * 查询topic列表
 * 
 * @author weibj
 *
 */
public class RMQListTopic extends RMQAdminOperation {
	protected boolean ignoreException = true;
	protected String tag = "data";

	public RMQListTopic(String tag, Logiclet p) {
		super(tag, p);
	}

	@Override
	public void configure(Properties p) {
		super.configure(p);
		
		tag = PropertiesConstants.getRaw(p, "tag", tag);
		ignoreException = PropertiesConstants.getBoolean(p, "ignoreException", true);
	}

	@Override
	protected void onExecute(MQAdminConnector adminConn, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		root.put(tag, adminConn.listTopic(ignoreException));
	}

}
