desc-topic
===========

desc-topic用于描述主题。

### 实现类

com.alogic.xscript.rocketmq.admin.RMQDescTopic

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | topic | 主题名称 |
| 2 | type | 查看topic路由还是统计信息，route为路由，status为统计信息，默认值为status|
| 3 | tag | 元素返回数据的标签 |
| 4 | ignoreException | 是否忽略异常，缺省值为false |

### 案例

```xml

	<?xml version="1.0"?>
	
	<script>
	
		<using xmlTag="admin-conn" module="com.alogic.xscript.rocketmq.admin.RMQAdminConn" />

		<admin-conn connectString="127.0.0.1:9876">	
	
			<desc-topic topic="testXscript" type="status" tag="descTopic"></desc-topic>
	
		</admin-conn>

	</script>

```