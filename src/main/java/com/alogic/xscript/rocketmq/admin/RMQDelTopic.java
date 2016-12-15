package com.alogic.xscript.rocketmq.admin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.rocketmq.util.MQAdminConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * 删除topic
 * 
 * @author weibj
 *
 */
public class RMQDelTopic extends RMQAdminOperation {
	//主题名称
	protected String topic = "";
	//在broker还是nameServer删除主题
	protected String type = "nameServer";
	//服务器地址
	protected String addrs = "";
	//分隔符
	protected String delimeter = ";";
	protected boolean ignoreException = false;

	public RMQDelTopic(String tag, Logiclet p) {
		super(tag, p);
	}

	@Override
	public void configure(Properties p) {
		super.configure(p);

		topic = PropertiesConstants.getRaw(p, "topic", topic);
		type = PropertiesConstants.getRaw(p, "type", type);
		addrs = PropertiesConstants.getRaw(p, "addrs", addrs);
		delimeter = PropertiesConstants.getRaw(p, "delimeter", delimeter);
		ignoreException = PropertiesConstants.getBoolean(p, "ignoreException", ignoreException);
	}

	@Override
	protected void onExecute(MQAdminConnector adminConn, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		String topicValue = ctx.transform(topic).trim();
		String[] items = ctx.transform(addrs).split(delimeter);
		Set<String> addrsValue = new HashSet<String>();
		
		for (String item : items) {
			addrsValue.add(item);
		}

		if (StringUtils.isNotEmpty(topicValue)) {
			adminConn.delTopic(addrsValue, topicValue, type, ignoreException);
		}
	}

}
