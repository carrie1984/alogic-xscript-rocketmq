package com.alogic.xscript.rocketmq;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.rocketmq.util.MQAdminConnector;
import com.anysoft.util.BaseException;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * 管理命令虚基类
 * @author weibj
 *
 */
public abstract class RMQAdminOperation extends AbstractLogiclet{
protected String pid = "$admin-conn";
	
	/**
	 * 返回结果的id
	 */
	protected String id;
	
	public RMQAdminOperation(String tag, Logiclet p) {
		super(tag, p);
	}
	
	public void configure(Properties p){
		super.configure(p);
		pid = PropertiesConstants.getString(p,"pid", pid,true);
		id = PropertiesConstants.getString(p,"id", "$" + getXmlTag(),true);
	}

	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		MQAdminConnector conn = ctx.getObject(pid);
		if (conn == null){
			throw new BaseException("core.no_adminconn","It must be in a admin-conn context,check your script.");
		}
		
		if (StringUtils.isNotEmpty(id)){
			onExecute(conn,root,current,ctx,watcher);
		}
	}

	protected abstract void onExecute(MQAdminConnector row, Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher);

}
