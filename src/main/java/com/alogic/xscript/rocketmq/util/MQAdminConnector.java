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
	protected String server = "${connectString}";

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

	/**
	 * 在指定broker或集群创建订阅组
	 * 
	 * @param addr
	 * @param config
	 * @param ignoreException
	 */
	public void createAndUpdateSubscriptionGroupConfig(String addr, SubscriptionGroupConfig config,
			boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
		}

		try {
			admin.createAndUpdateSubscriptionGroupConfig(addr, config);
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
	 * 在指定broker或集群删除订阅组
	 * 
	 * @param addr
	 * @param groupName
	 * @param ignoreException
	 */
	public void deleteSubscriptionGroup(String addr, String groupName, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
		}

		try {
			admin.deleteSubscriptionGroup(addr, groupName);
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
	 * 更新Broker配置文件
	 * 
	 * @param brokerAddr
	 * @param properties
	 * @param ignoreException
	 */
	public void updateBrokerConfig(String brokerAddr, java.util.Properties properties, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
		}

		try {
			admin.updateBrokerConfig(brokerAddr, properties);
		} catch (UnsupportedEncodingException e) {
			if (!ignoreException)
				throw new BaseException("rmq.UnsupportedEncodingException", e.getMessage(), e);
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
	 * 查看broker统计信息
	 * 
	 * @param brokerAddr
	 * @param ignoreException
	 * @return
	 */
	public KVTable fetchBrokerRuntimeStats(final String brokerAddr, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			KVTable table = admin.fetchBrokerRuntimeStats(brokerAddr);
			return table;
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
		return null;
	}

	/**
	 * 根据消息ID查询消息
	 * 
	 * @param msgId
	 * @param ignoreException
	 * @return
	 */
	public MessageExt viewMessage(String msgId, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			MessageExt msg = admin.viewMessage(msgId);
			return msg;
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
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
		return null;
	}

	/**
	 * 根据消息Key或Offset查询消息
	 * 
	 * @param topic
	 * @param key
	 * @param maxNum
	 * @param begin
	 * @param end
	 * @return
	 */
	public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end,
			boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			QueryResult queryResult = admin.queryMessage(topic, key, maxNum, begin, end);
			return queryResult;
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (InterruptedException e) {
			if (!ignoreException)
				throw new BaseException("rmq.InterruptedException", e.getMessage(), e);
		}
		return null;
	}

	/**
	 * 查询Produer的网络连接
	 * 
	 * @param producerGroup
	 * @param topic
	 * @return
	 */
	public ProducerConnection examineProducerConnectionInfo(String producerGroup, final String topic,
			boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			ProducerConnection conn = admin.examineProducerConnectionInfo(producerGroup, topic);
			return conn;
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
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
		return null;
	}

	/**
	 * 查询Consumer的网络连接
	 * 
	 * @param consumerGroup
	 * @return
	 */
	public ConsumerConnection examineConsumerConnectionInfo(String consumerGroup, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			ConsumerConnection conn = admin.examineConsumerConnectionInfo(consumerGroup);
			return conn;
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
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
		return null;
	}

	/**
	 * 查看订阅组消费状态
	 * 
	 * @param consumerGroup
	 * @param ignoreException
	 * @return
	 */
	public ConsumeStats examineConsumeStats(String consumerGroup, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			ConsumeStats stats = admin.examineConsumeStats(consumerGroup);
			return stats;
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
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
		return null;
	}

	/**
	 * 查看集群信息
	 * 
	 * @param ignoreException
	 * @return
	 */
	public ClusterInfo examineBrokerClusterInfo(boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			ClusterInfo clusterInfo = admin.examineBrokerClusterInfo();
			return clusterInfo;
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
		return null;
	}

	/**
	 * 添加（更新）KV配置信息
	 * 
	 * @param namespace
	 * @param key
	 * @param value
	 * @param ignoreException
	 */
	public void createAndUpdateKvConfig(String namespace, String key, String value, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
		}

		try {
			admin.createAndUpdateKvConfig(namespace, key, value);
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
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
	 * 刪除KV配置信息
	 * 
	 * @param namespace
	 * @param key
	 * @param ignoreException
	 */
	public void deleteKvConfig(String namespace, String key, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
		}

		try {
			admin.deleteKvConfig(namespace, key);
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
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
	 * 设置消费进度
	 * 
	 * @param consumerGroup
	 * @param topic
	 * @param timestamp
	 */
	public void resetOffsetNew(String consumerGroup, String topic, long timestamp, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
		}

		try {
			admin.resetOffsetNew(consumerGroup, topic, timestamp);
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
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
	 * 清除特定Broker权限
	 * 
	 * @param namesrvAddr
	 * @param brokerName
	 * @return
	 */
	public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return 0;
		}

		try {
			int result = admin.wipeWritePermOfBroker(namesrvAddr, brokerName);
			return result;
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (RemotingException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (InterruptedException e) {
			if (!ignoreException)
				throw new BaseException("rmq.InterruptedException", e.getMessage(), e);
		}
		return 0;
	}

	/**
	 * 查询Consumer消费进度
	 * @param consumerGroup
	 * @param topic
	 * @param ignoreException
	 * @return
	 */
	public ConsumeStats examineConsumeStats(String consumerGroup, String topic, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("admin.noconn", "the admin is not connected.");
			logger.error("the admin is not connected.");
			return null;
		}

		try {
			ConsumeStats stats = admin.examineConsumeStats(consumerGroup, topic);
			return stats;
		} catch (MQBrokerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MQClientException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (RemotingException e) {
			if (!ignoreException)
				throw new BaseException("rmq.RemotingException", e.getMessage(), e);
		} catch (InterruptedException e) {
			if (!ignoreException)
				throw new BaseException("rmq.InterruptedException", e.getMessage(), e);
		}
		return null;
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
