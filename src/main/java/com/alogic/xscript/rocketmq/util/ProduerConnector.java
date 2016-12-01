package com.alogic.xscript.rocketmq.util;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.anysoft.util.BaseException;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/**
 * RocketMQ的生产者
 * 
 * @author weibj
 *
 */
public class ProduerConnector {
	/**
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(ProduerConnector.class);

	/**
	 * RocketMQ Producer的连接参数
	 */
	protected String server = "${server}";
	protected String producerGroup = "${producer.group}";

	/**
	 * Producer
	 */
	protected DefaultMQProducer producer = null;

	public ProduerConnector(Properties props) {
		server = PropertiesConstants.getString(props, "server", server);
		producerGroup = PropertiesConstants.getString(props, "producerGroup", producerGroup);

		connect();
	}

	public ProduerConnector(Properties props, String servers, String producerGroups) {
		server = servers;
		producerGroup = producerGroups;

		connect();
	}

	/**
	 * 连接Producer
	 */
	public void connect() {
		try {
			producer = new DefaultMQProducer(producerGroup);
			producer.setNamesrvAddr(server);

			producer.start();
		} catch (MQClientException e) {
			logger.error("Can not connect to nameserver:" + server);
		}
	}

	/**
	 * 关闭Producer
	 */
	public void disconnect() {
		if (isConnected()) {
			producer.shutdown();
			producer = null;
		}
	}

	/**
	 * 是否已经连接
	 * 
	 * @return true|false
	 */
	public boolean isConnected() {
		return producer != null;
	}

	/**
	 * 重新连接
	 */
	public void reconnect() {
		disconnect();
		connect();
	}

	/**
	 * 生产者发送消息
	 * 
	 * @param topic
	 * @param tags
	 * @param key
	 * @param data
	 * @param ignoreException
	 * @return
	 */
	public String sendMsg(String topic, String tags, String key, String data, boolean ignoreException) {
		if (!isConnected()) {
			if (!ignoreException)
				throw new BaseException("producer.noconn", "the producer is not connected.");
			logger.error("the producer is not connected.");
			return null;
		}

		try {
			Message msg = new Message(topic, tags, key, data.getBytes());
			SendResult sendResult = producer.send(msg);
			return sendResult.toString();
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

}
