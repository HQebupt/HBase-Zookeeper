package hq.zk;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * 2. 创建一个复用sessionId和sessionPasswd的ZooKeeper对象示例
 */
public class ZKDemo2 {
	private static final String CONNECTION_STRING = "eb177:2181,eb178:2181,eb179:2181";

	public static void main(String[] args) throws Exception {

		final CountDownLatch connectedSignal = new CountDownLatch(1);
		ZooKeeper zk = new ZooKeeper(CONNECTION_STRING, 5000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("收到事件通知：" + event.getState() + "\n");
				if (event.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});

		connectedSignal.await();
		long sessionId = zk.getSessionId();
		byte[] passwd = zk.getSessionPasswd();
		System.out.println("sessionId:" + sessionId + "\n" + "SessionPasswd:"
				+ new String(passwd));
		// zk.close();  // 有这句话，连接无法复用；下面的状态WatchedEvent是Expire状态

		final CountDownLatch anotherConnectedSignal = new CountDownLatch(1);
		ZooKeeper newZk = new ZooKeeper(CONNECTION_STRING, 5000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("收到事件通知：" + event.getState() + "\n");
				if (event.getState() == KeeperState.SyncConnected) {
					anotherConnectedSignal.countDown();
				}
			}
		}, sessionId, passwd);

		anotherConnectedSignal.await();
		System.out.println("sessionId被连接复用.");
		newZk.close();
	}

}