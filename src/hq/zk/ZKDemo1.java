package hq.zk;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * 1. 创建一个基本的ZooKeeper会话实例
 * @created 2015-3-16
 */
public class ZKDemo1 {
	private static final String CONNECTION_STRING = "eb177:2181,eb178:2181,eb179:2181";

	public static void main(String[] args) throws Exception {
		final CountDownLatch connectedSignal = new CountDownLatch(1);
		/**
		 * ZooKeeper客户端和服务器会话的建立是一个异步的过程
		 * 构造函数在处理完客户端的初始化工作后立即返回，在大多数情况下，并没有真正地建立好会话
		 * 当会话真正创建完毕后，Zookeeper服务器会向客户端发送一个事件通知
		 */
		ZooKeeper zk = new ZooKeeper(CONNECTION_STRING, 5000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("收到事件通知：" + event.getState() + "\n");
				if (event.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});
		System.out.println("State1: " + zk.getState()); // CONNECTING

		connectedSignal.await(); // 必须等到连接上了，才释放阻塞。
		System.out.println("State2: " + zk.getState()); // CONNECTED

		zk.close();
		System.out.println("State3: " + zk.getState()); // CLOSED
	}

}