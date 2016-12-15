receive
=======

receive用于接收消息。

### 实现类

com.alogic.xscript.rocketmq.RMQReceive

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | pid | 父节点的元素名称，默认为$consumer-conn |
| 2 | id | 当前元素的id |
| 3 | size | pull模式接收消息的条数 |
| 4 | tag | 元素返回结果的标签  |
| 5 | ignoreException | 是否忽略异常，缺省值为false |

### 案例

```

	<?xml version="1.0"?>
	<script>
	
		<using xmlTag="consumer-conn" module="com.alogic.xscript.rocketmq.RMQConsumerConn" />
		
		<consumer-conn server="127.0.0.1:9876" topic="test" delimeter=";" consumerGroup="TestConsumer" tags="*" type="push"> 
			<receive id="msg" />
		</consumer-conn>
		
	</script>

```