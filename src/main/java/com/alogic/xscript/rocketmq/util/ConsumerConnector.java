package com.alogic.xscript.rocketmq.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.anysoft.util.BaseException;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * RocketMQ的消费者
 * 
 * @author weibj
 *
 */
public class ConsumerConnector {
	/**
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(ConsumerConnector.class);

	/**
	 * RocketMQ Consumer的连接参数
	 */
	protected String server = "${server}";
	protected String consumerGroup = "${consumer.group}";
	protected String tag = "${tags}";
	protected String topic = "${topic}";

	// Java缓存
	private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

	/**
	 * Consumer
	 */
	protected DefaultMQPullConsumer consumer = null;

	public ConsumerConnector(Properties props) {
		server = PropertiesConstants.getString(props, "server", server);
		consumerGroup = PropertiesConstants.getString(props, "consumerGroup", consumerGroup);

		connect();
	}

	public ConsumerConnector(Properties props, String servers, String topics, String consumerGroups, String tags) {
		server = servers;
		consumerGroup = consumerGroups;
		topic = topics;
		tag = tags;

		connect();
	}

	/**
	 * 连接Consumer
	 */
	public void connect() {
		consumer = new DefaultMQPullConsumer();
		consumer.setNamesrvAddr(server);
		consumer.setConsumerGroup(consumerGroup);
	}

	/**
	 * 关闭Consumer
	 */
	public void disconnect() {
		if (isConnected()) {
			consumer.shutdown();
			consumer = null;
		}
	}

	/**
	 * 是否已经连接
	 * 
	 * @return true|false
	 */
	public boolean isConnected() {
		return consumer != null;
	}

	/**
	 * 重新连接
	 */
	public void reconnect() {
		disconnect();
		connect();
	}

	public List<String> receiveMsg(int size, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("consumer.noconn", "the consumer is not connected.");
			logger.error("the consumer is not connected.");
			return null;
		}

		List<String> result = new ArrayList<String>();

		try {
			consumer.start();
			Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues(topic);

			for (MessageQueue mq : messageQueues) {

				PullResult pullResult;
				pullResult = consumer.pull(mq, null, getMessageQueueOffset(mq), size);
				List<MessageExt> list = pullResult.getMsgFoundList();
				if (list != null) {
					for (MessageExt msg : list) {
						result.add(new String(msg.getBody()));
					}
				}
				putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
			}
			return result;
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
		return null;
	}

	private static void putMessageQueueOffset(MessageQueue mq, long offset) {
		offseTable.put(mq, offset);
	}

	private static long getMessageQueueOffset(MessageQueue mq) {
		Long offset = offseTable.get(mq);
		if (offset != null) {
			return offset;
		}
		return 0;
	}

}
