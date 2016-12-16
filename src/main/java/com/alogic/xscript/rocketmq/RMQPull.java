package com.alogic.xscript.rocketmq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.alogic.xscript.rocketmq.util.ConsumerConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class RMQPull extends Segment{
	/**
	 * consumer的pid
	 */
	private String pid = "$mq-puller";
	
	// max pulling numbers
	protected int size = 1;
	
	// 返回数据的标签
	protected String tag = "data";
	
	// 是否忽略异常
	protected boolean ignoreException = false;
	
	/**
	 * 返回结果的id
	 */
	protected String id;

	public RMQPull(String tag, Logiclet p) {
		super(tag, p);
	}

	public void configure(Properties p) {
		super.configure(p);
		pid = PropertiesConstants.getString(p, "pid", pid);
		id = PropertiesConstants.getString(p, "id", "$" + getXmlTag());
		size = PropertiesConstants.getInt(p, "size", size);
		tag = PropertiesConstants.getRaw(p, "tag", tag);
		ignoreException = PropertiesConstants.getBoolean(p, "ignoreException", ignoreException);
	}

	@Override
	protected void onExecute(Map<String, Object> root, Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		ConsumerConnector conn = ctx.getObject(pid);

		List<String> result = new ArrayList<String>();
		result = conn.receivePullMsg(size, ignoreException);
		root.put(tag, result);
		if (result.size() > 0){
			for (String value:result){
				ctx.SetValue(id, value);
				super.onExecute(root, current, ctx, watcher);
			}
		}
	}
	
}
