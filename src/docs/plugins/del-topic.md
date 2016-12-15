del-topic
===========

del-topic用于在指定broker或集群删除主题。

### 实现类

com.alogic.xscript.rocketmq.admin.RMQDelTopic

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | topic | 主题名称 |
| 2 | addrs | broker地址，多个地址以delimeter分隔 |
| 3 | type | type为nameServer，则在NameServer删除topic；type为broker，则在Broker删除topic；默认值为nameServer|
| 4 | delimeter | 分隔符，默认值为; |
| 5 | ignoreException | 是否忽略异常，缺省值为false |

### 案例

```xml

	<?xml version="1.0"?>
	
	<script>
	
		<using xmlTag="admin-conn" module="com.alogic.xscript.rocketmq.admin.RMQAdminConn" />

		<admin-conn connectString="127.0.0.1:9876">	
	
			<del-topic topic="testXscript" type="nameServer" addrs="127.0.0.1:9876"></del-topic>
	
		</admin-conn>

	</script>

```