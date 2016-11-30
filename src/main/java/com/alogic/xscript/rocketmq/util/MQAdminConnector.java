package com.alogic.xscript.rocketmq.util;
import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;

public class MQAdminConnector {
	
	public static void main(String[] args) {
		ClientConfig cc = new ClientConfig();
		cc.setNamesrvAddr("127.0.0.1:9876");
		
		DefaultMQAdminExt ext = new DefaultMQAdminExt();
		ext.setNamesrvAddr("127.0.0.1:9876");
		try {
			ext.start();
			ext.createTopic("test", "testCreatehohoho", 1);
			ext.shutdown();
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
