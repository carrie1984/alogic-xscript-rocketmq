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
	protected String type = "push";
	protected String subExpression = "*";

	// Java缓存
	private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

	/**
	 * Consumer
	 */
	protected DefaultMQPullConsumer pullConsumer = null;
	protected DefaultMQPushConsumer pushConsumer = null;

	public ConsumerConnector(Properties props) {
		server = PropertiesConstants.getString(props, "server", server);
		tag = PropertiesConstants.getString(props, "tag", tag);
		topic = PropertiesConstants.getString(props, "topic", topic);
		type = PropertiesConstants.getString(props, "type", type);
		subExpression = PropertiesConstants.getString(props, "subExpression", subExpression);
		consumerGroup = PropertiesConstants.getString(props, "consumerGroup", consumerGroup);

		connect();
	}

	public ConsumerConnector(Properties props, String servers, String topics, String consumerGroups, String tags,String types) {
		server = servers;
		consumerGroup = consumerGroups;
		topic = topics;
		tag = tags;
		type = types;

		connect();
	}

	/**
	 * 连接Consumer
	 */
	public void connect() {
		switch (type) {
		case "pull":
			pullConsumer = new DefaultMQPullConsumer();
			pullConsumer.setNamesrvAddr(server);
			pullConsumer.setConsumerGroup(consumerGroup);
			break;
		case "push":
			pushConsumer = new DefaultMQPushConsumer();
			pushConsumer.setNamesrvAddr(server);
			pushConsumer.setConsumerGroup(consumerGroup);
			try {
				pushConsumer.subscribe(topic, subExpression);
			} catch (MQClientException e) {
				e.printStackTrace();
			}
			break;
		default:
			break;
		}
	}

	/**
	 * 关闭Consumer
	 */
	public void disconnect() {
		if (isConnected()) {
			switch (type) {
			case "pull":
				pullConsumer.shutdown();;
				pullConsumer=null;
				break;
			case "push":
				pushConsumer.shutdown();;
				pushConsumer=null;
				break;
			default:
				break;
			}
		}
	}

	/**
	 * 是否已经连接
	 * 
	 * @return true|false
	 */
	public boolean isConnected() {
		switch (type) {
		case "pull":
			return pullConsumer != null;
		case "push":
			return pushConsumer != null;
		default:
			return false;
		}
	}

	/**
	 * 重新连接
	 */
	public void reconnect() {
		disconnect();
		connect();
	}
	
	/**
	 * 返回push模式Consumer
	 * @return
	 */
	public DefaultMQPushConsumer getPushConsumer()
	{
		return pushConsumer;
	}
	
	public String getCurrentType()
	{
		return type;
	}

	public List<String> receivePullMsg(int size, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("consumer.noconn", "the consumer is not connected.");
			logger.error("the consumer is not connected.");
			return null;
		}

		List<String> result = new ArrayList<String>();

		try {
			pullConsumer.start();
			Set<MessageQueue> messageQueues = pullConsumer.fetchSubscribeMessageQueues(topic);

			for (MessageQueue mq : messageQueues) {

				PullResult pullResult;
				pullResult = pullConsumer.pull(mq, null, getMessageQueueOffset(mq), size);
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
