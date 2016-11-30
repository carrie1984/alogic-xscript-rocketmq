package com.alogic.xscript.rocketmq;

import java.util.Map;

import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.alogic.xscript.rocketmq.util.ProduerConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class RMQProdConn extends Segment{
	protected String cid = "$prod-conn";
	protected String server = "${server}";
	protected String producerGroup = "${producer.group}";
	
	public RMQProdConn(String tag, Logiclet p) {
		super(tag, p);	
		registerModule("send",RMQSend.class);
	}

	@Override
	public void configure(Properties p){
		super.configure(p);
		
		cid = PropertiesConstants.getString(p,"cid",cid,true);
		server = PropertiesConstants.getString(p,"server",server,true);
		producerGroup = PropertiesConstants.getString(p,"producerGroup",producerGroup,true);
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx, ExecuteWatcher watcher) {
		ProduerConnector conn = new ProduerConnector(ctx,server,producerGroup);
		try {
			ctx.setObject(cid, conn);
			System.out.println("+++++++++++++++++++++++已经连上！");
			super.onExecute(root, current, ctx, watcher);
		}finally{
			ctx.removeObject(cid);
			conn.disconnect();
		}
	}
}
