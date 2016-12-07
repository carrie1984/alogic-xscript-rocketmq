package com.alogic.xscript.rocketmq.admin;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
 * 描述指定topic信息
 * 
 * @author weibj
 *
 */
public class RMQDescTopic extends RMQAdminOperation {
	protected String topic = "";
	protected String type = "status";
	protected boolean ignoreException;
	protected String tag = "data";

	public RMQDescTopic(String tag, Logiclet p) {
		super(tag, p);
	}

	@Override
	public void configure(Properties p) {
		super.configure(p);

		topic = PropertiesConstants.getRaw(p, "topic", topic);
		type = PropertiesConstants.getRaw(p, "type", type);
		tag = PropertiesConstants.getRaw(p, "tag", tag);
		ignoreException = PropertiesConstants.getBoolean(p, "ignoreException", true);
	}

	@Override
	protected void onExecute(MQAdminConnector adminConn, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		String topicValue = ctx.transform(topic).trim();

		if (StringUtils.isNotEmpty(topicValue)) {
			switch (type) {
			case "status":
				TopicStatsTable table = adminConn.topicStatus(topicValue, ignoreException);
				root.put(tag, table.toJson());
				break;
			case "route":
				TopicRouteData route = adminConn.topicRoute(topicValue, ignoreException);
				root.put(tag, route.toJson());
				break;
			default:
				break;
			}
		}
	}

}
