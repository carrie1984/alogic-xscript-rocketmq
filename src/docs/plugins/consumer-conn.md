consumer-conn
============

consumer-conn用于创建一个Consumer连接。

### 实现类

com.alogic.xscript.rocketmq.RMQConsumerConn

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | cid | connector的上下文对象id，缺省值为$consumer-conn |
| 2 | server | NameServer的连接地址：[ip:port]，缺省值为${server}，多个地址以“;”分隔 |
| 3 | consumerGroup | 生产者组名 |
| 4 | topic | 消费消息的主题 |
| 5 | tag | 消息标签 |
| 6 | type | 消费类型， type为push，则采用PushConsumer进行消息消费，应用通常向Consumer注册一个Listener监听器，Consumer收到消息立刻回调Listener监听器；type为pull，则采用PullConsumer进行消息消费，应用通常主动调用Consumer的拉取消息方法从Broker拉消息，主动权由应用控制 |


### 案例
可以在conf/setting中设置参数${server}来指定nameserver连接地址,${producer.group}来指定生产者组名。
```xml

	<?xml version="1.0"?>
	<settings>
	
		<parameter id="server" value="127.0.0.1:9876"/>
		<parameter id="producer.group" value="Order"/>
		
	</settings>

```

也可以在脚本中指定nameServer连接地址

```xml

	<?xml version="1.0"?>
	<script>
	
		<using xmlTag="prod-conn" module="com.alogic.xscript.rocketmq.RMQProdConn" />
		
		<prod-conn server="127.0.0.1:9876" producerGroup="TestProducer" />
		
	</script>
	
```