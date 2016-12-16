mq-rocket
============

mq-rocket用于创建一个命名空间。

### 实现类

com.alogic.xscript.rocketmq.MQRocket

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | cid | connector的上下文对象id，缺省值为$mq-rocket |
| 2 | server | NameServer的连接地址：[ip:port]，缺省值为${server}，多个地址以“;”分隔 |


### 案例
可以在conf/setting中设置参数${server}来指定nameserver连接地址。
```xml

	<?xml version="1.0"?>
	<settings>
	
		<parameter id="server" value="127.0.0.1:9876"/>
		
	</settings>

```

也可以在脚本中指定nameServer连接地址

```xml

	<?xml version="1.0"?>
	<script>
		<!-- 引用mq-rocket指令的实现类 -->
		<using xmlTag="mq-rocket" module="com.alogic.xscript.rocketmq.MQRocket" />
	
		<mq-rocket>
		</mq-rocket>
	</script>
	    
```