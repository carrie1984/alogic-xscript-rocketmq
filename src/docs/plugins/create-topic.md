create-topic
===========

create-topic用于在指定broker或集群新增主题。

### 实现类

com.alogic.xscript.rocketmq.admin.RMQUpdateTopic

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | topic | 主题名称 |
| 2 | addr | broker地址 |
| 3 | readQueueNums | 可读队列数 |
| 4 | writeQueueNums | 可写队列数 |
| 5 | perm | 新增topic的权限限制 |
| 6 | ignoreException | 是否忽略异常，缺省值为false |

### 案例

```xml
	
	<?xml version="1.0"?>
	<script>
		<!-- 引用mq-rocket指令的实现类 -->
		<using xmlTag="mq-rocket" module="com.alogic.xscript.rocketmq.MQRocket" />
	
		<mq-rocket>
			<mq-admin server="127.0.0.1:9876">
				<create-topic topic="testXscript" perm="2" readQueueNums="2" writeQueueNums="2" addr="127.0.0.1:10911"/>
			</mq-admin>
		</mq-rocket>
    </script>

```