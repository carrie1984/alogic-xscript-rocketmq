import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQAdminImpl;
import com.alibaba.rocketmq.client.impl.MQClientAPIImpl;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

public class TestAdmin {
	public static void main(String[] args) {
		ClientConfig cc = new ClientConfig();
		cc.setNamesrvAddr("127.0.0.1:9876");
		MQClientInstance mq = new MQClientInstance(cc,0,"0");
		try {
			mq.start();
			MQClientAPIImpl impl = mq.getMQClientAPIImpl();
			MQAdminImpl admin = mq.getMQAdminImpl();
			TopicConfig con = new TopicConfig("testCreate");
			admin.createTopic("", "testCreate11", 1);
			System.out.println("哇！创建成功啦~~");
			mq.shutdown();
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
