package com.alogic.xscript.rocketmq.admin;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.rocketmq.util.MQAdminConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class RMQUpdateTopic extends RMQAdminOperation{
	protected String topic = "";
	protected String addr = "";
	protected int readQueueNums = 16;
	protected int writeQueueNums = 16;
	protected int perm = PermName.PERM_READ | PermName.PERM_WRITE;
	protected boolean ignoreException;

	public RMQUpdateTopic(String tag, Logiclet p) {
		super(tag, p);
	}

	@Override
	public void configure(Properties p) {
		super.configure(p);

		topic = PropertiesConstants.getRaw(p, "topic", topic);
		addr = PropertiesConstants.getRaw(p, "addr", addr);
		readQueueNums=PropertiesConstants.getInt(p, "readQueueNums", readQueueNums);
		writeQueueNums=PropertiesConstants.getInt(p, "writeQueueNums", writeQueueNums);
		perm=PropertiesConstants.getInt(p, "perm", perm);
		ignoreException = PropertiesConstants.getBoolean(p, "ignoreException", true);
	}

	@Override
	protected void onExecute(MQAdminConnector adminConn, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		String topicValue = ctx.transform(topic).trim();
		TopicConfig config = new TopicConfig();
		config.setTopicName(topicValue);
		config.setPerm(perm);
		config.setReadQueueNums(readQueueNums);
		config.setWriteQueueNums(writeQueueNums);

		if (StringUtils.isNotEmpty(topicValue)) {
			adminConn.createAndUpdateTopic(addr, config, ignoreException);
		}
	}
}
