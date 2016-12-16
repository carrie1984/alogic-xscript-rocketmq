mq-send
=======

mq-send用于发送消息。

### 实现类

com.alogic.xscript.rocketmq.RMQSend

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | pid | 父节点的元素名称，默认为$mq-sender |
| 2 | topic | 发送消息的主题 |
| 3 | tags | 消息标签，只支持设置一个Tag（服务端消息过滤使用） |
| 4 | key | 消息关键词，多个Key用KEY_SEPARATOR隔开（查询消息使用）  |
| 5 | data | 发送数据 |
| 6 | ignoreException | 是否忽略异常，缺省值为false |

### 案例

```

	<?xml version="1.0"?>
	<script>
		<!-- 引用mq-rocket指令的实现类 -->
		<using xmlTag="mq-rocket" module="com.alogic.xscript.rocketmq.MQRocket" />
	
		<mq-rocket>
			<!-- 连接生产者 -->
			<mq-sender server="127.0.0.1:9876" producerGroup="TestProducer">
				
				<!-- 发送消息 -->
				<mq-send topic="test" tags="" key="" data="hello"></mq-send>
				
			</mq-sender>
		</mq-rocket>
	</script>

```