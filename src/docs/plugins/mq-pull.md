mq-pull
=======

mq-pull用于接收消息。

### 实现类

com.alogic.xscript.rocketmq.RMQPull

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | pid | 父节点的元素名称，默认为$mq-puller |
| 2 | id | 当前元素的id |
| 3 | size | pull模式接收消息的条数 |
| 4 | tag | 元素返回结果的标签  |
| 5 | ignoreException | 是否忽略异常，缺省值为false |

### 案例

```

	<?xml version="1.0"?>
	<script>
		<!-- 引用mq-rocket指令的实现类 -->
		<using xmlTag="mq-rocket" module="com.alogic.xscript.rocketmq.MQRocket" />
	
		<mq-rocket>
			<!-- 连接消费者 -->
			<mq-puller server="127.0.0.1:9876" topic="test"
			consumerGroup="TestConsumer" tags="*">
				<!-- 接收消息 -->
				<mq-pull id="msg">
					<!-- 调用子指令，对消息进行处理 -->
					<get id="${msg}" value="${msg}"></get>
				</mq-pull>
			</mq-puller>
		</mq-rocket>
	</script>

```