list-topic
===========

list-topic用于获取主题列表。

### 实现类

com.alogic.xscript.rocketmq.admin.RMQListTopic

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | tag | 元素返回数据的标签 |
| 2 | ignoreException | 是否忽略异常，缺省值为false |

### 案例

```xml

	<?xml version="1.0"?>
	<script>
		<!-- 引用mq-rocket指令的实现类 -->
		<using xmlTag="mq-rocket" module="com.alogic.xscript.rocketmq.MQRocket" />
	
		<mq-rocket>
		
			<mq-admin server="127.0.0.1:9876">
			
				<list-topic></list-topic>
				
			</mq-admin>
			
		</mq-rocket>
    </script>

```