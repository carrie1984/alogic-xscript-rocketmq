package com.alogic.xscript.rocketmq.util;

import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.common.protocol.body.ProducerConnection;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.anysoft.util.BaseException;

import java.io.UnsupportedEncodingException;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * RocketMQ的管理者
 * 
 * @author weibj
 *
 */
public class MQAdminConnector {
	/**
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(MQAdminConnector.class);

	/**
	 * RocketMQ Admin的连接参数
	 */
	protected String server = "${server}";

	/**
	 * Admin
	 */
	protected DefaultMQAdminExt admin = null;

	public MQAdminConnector(Properties props) {
		server = PropertiesConstants.getString(props, "server", server);

		connect();
	}

	public MQAdminConnector(Properties props, String servers) {
		server = servers;

		connect();
	}

	/**
	 * 连接Admin
	 */
	public void connect() {
		try {
			admin = new DefaultMQAdminExt();
			admin.setNamesrvAddr(server);

			admin.start();
		} catch (MQClientException e) {
			logger.error("Can not connect to nameserver:" + server);
		}
	}

	/**
	 * 关闭Admin
	 */
	public void disconnect() {
		if (isConnected()) {
			admin.shutdown();
			admin = null;
		}
	}

	/**
	 * 是否已经连接
	 * 
	 * @return true|false
	 */
	public boolean isConnected() {
		return admin != null;
	}

	/**
	 * 重新连接
	 */
	public void reconnect() {
		disconnect();
		connect();
	}

	/**
	 * 在指定broker或集群创建或修改主题
	 * 
	 * @param addr
	 * @param config
	 * @param ignoreException
	 */
	public void createAndUpdateTopic(String addr, TopicConfig config, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
		}

		try {
			admin.createAndUpdateTopicConfig(addr, config);
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.ClientException", e.getMessage(), e);
		} catch (RemotingException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (MQBrokerException e) {
			if (!ignoreException)
				throw new BaseException("rmq.BrokerException", e.getMessage(), e);
		} catch (InterruptedException e) {
			if (!ignoreException)
				throw new BaseException("rmq.InterruptedException", e.getMessage(), e);
		}
	}

	/**
	 * 查看topic列表信息
	 * 
	 * @param ignoreException
	 * @return
	 */
	public TopicList listTopic(boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			return admin.fetchAllTopicList();
		} catch (RemotingException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.ClientException", e.getMessage(), e);
		} catch (InterruptedException e) {
			if (!ignoreException)
				throw new BaseException("rmq.InterruptedException", e.getMessage(), e);
		}
		return null;
	}

	/**
	 * 描述topic统计信息
	 * 
	 * @param topic
	 * @param ignoreException
	 * @return
	 */
	public TopicStatsTable topicStatus(String topic, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			return admin.examineTopicStats(topic);
		} catch (RemotingException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.ClientException", e.getMessage(), e);
		} catch (InterruptedException e) {
			if (!ignoreException)
				throw new BaseException("rmq.InterruptedException", e.getMessage(), e);
		} catch (MQBrokerException e) {
			if (!ignoreException)
				throw new BaseException("rmq.BrokerException", e.getMessage(), e);
		}
		return null;
	}

	/**
	 * 查看topic路由信息
	 * 
	 * @param topic
	 * @param ignoreException
	 * @return
	 */
	public TopicRouteData topicRoute(String topic, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			return admin.examineTopicRouteInfo(topic);
		} catch (RemotingException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.ClientException", e.getMessage(), e);
		} catch (InterruptedException e) {
			if (!ignoreException)
				throw new BaseException("rmq.InterruptedException", e.getMessage(), e);
		}
		return null;
	}

	/**
	 * 在指定nameServer或broker删除主题
	 * 
	 * @param addrs
	 * @param topic
	 * @param ignoreException
	 */
	public void delTopic(Set<String> addrs, String topic, String type, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
		}

		try {
			switch (type) {
			case "nameServer":
				admin.deleteTopicInNameServer(addrs, topic);
				break;
			case "broker":
				admin.deleteTopicInBroker(addrs, topic);
				break;
			default:
				break;
			}
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.ClientException", e.getMessage(), e);
		} catch (RemotingException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (MQBrokerException e) {
			if (!ignoreException)
				throw new BaseException("rmq.BrokerException", e.getMessage(), e);
		} catch (InterruptedException e) {
			if (!ignoreException)
				throw new BaseException("rmq.InterruptedException", e.getMessage(), e);
		}

	}

	public static void main(String[] args) {
		ClientConfig cc = new ClientConfig();
		cc.setNamesrvAddr("127.0.0.1:9876");

		TopicConfig tc = new TopicConfig();
		tc.setTopicName("testconfig");

		DefaultMQAdminExt ext = new DefaultMQAdminExt();
		ext.setNamesrvAddr("127.0.0.1:9876");
		try {
			ext.start();
			// ext.createTopic("testCreatehoho", "testCreate11", 1);
			ext.createAndUpdateTopicConfig("127.0.0.1:10911", tc);
			ext.shutdown();
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemotingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MQBrokerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
