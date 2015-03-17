package hq.zk;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * 11.ZooKeeper权限控制
 */
public class ZKDemo11 {
	private static final String CONNECTION_STRING = "eb177:2181,eb178:2181,eb179:2181";

	public static void main(String[] args) throws Exception {

		/**
		 * 使用含有权限信息的zookeeper会话创建数据节点
		 */
		final CountDownLatch connectedSignal = new CountDownLatch(1);
		ZooKeeper zk = new ZooKeeper(CONNECTION_STRING, 5000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("收到事件通知：" + event.getState() + "\n");
				connectedSignal.countDown();
			}
		});
		connectedSignal.await();
		zk.addAuthInfo("digest", "hq:123".getBytes());
		zk.create("/hq", "hello".getBytes(), Ids.CREATOR_ALL_ACL,
				CreateMode.PERSISTENT);
		// zk.delete("/hq", -1);
		zk.close();

		/**
		 * 使用无权限信息的zookeeper会话访问含有权限信息的数据节点
		 */
		try {
			final CountDownLatch signal = new CountDownLatch(1);
			ZooKeeper zk1 = new ZooKeeper(CONNECTION_STRING, 5000,
					new Watcher() {
						@Override
						public void process(WatchedEvent event) {
							System.out.println("收到事件通知：" + event.getState()
									+ "\n");
							signal.countDown();
						}
					});
			signal.await();
			zk1.getData("/hq", false, null);
			System.out.println("NodeData: "
					+ new String(zk1.getData("/hq", false, null)));
			zk1.close();
		} catch (Exception e) {
			System.out.println("含有权限信息Failed to delete Znode: "
					+ e.getMessage());
		}

		/**
		 * 使用错误权限信息的zookeeper会话访问含有权限信息的数据节点
		 */
		try {
			final CountDownLatch signal = new CountDownLatch(1);
			ZooKeeper zk2 = new ZooKeeper(CONNECTION_STRING, 5000,
					new Watcher() {
						@Override
						public void process(WatchedEvent event) {
							System.out.println("收到事件通知：" + event.getState()
									+ "\n");
							signal.countDown();
						}
					});
			signal.await();
			zk2.addAuthInfo("digest", "hq:abc".getBytes());
			System.out.println("NodeData: "
					+ new String(zk2.getData("/hq", false, null)));
			zk2.close();
		} catch (Exception e) {
			System.out.println("错误权限信息Failed to delete Znode: "
					+ e.getMessage());
		}

		/**
		 * 使用正确权限信息的zookeeper会话访问含有权限信息的数据节点
		 */
		try {
			final CountDownLatch signal = new CountDownLatch(1);
			ZooKeeper zk3 = new ZooKeeper(CONNECTION_STRING, 5000,
					new Watcher() {
						@Override
						public void process(WatchedEvent event) {
							System.out.println("收到事件通知：" + event.getState()
									+ "\n");
							signal.countDown();
						}
					});
			signal.await();
			zk3.addAuthInfo("digest", "hq:123".getBytes());
			System.out.println("正确权限信息NodeData: "
					+ new String(zk3.getData("/hq", false, null)));
			zk3.close();
		} catch (Exception e) {
			System.out.println("Failed to delete Znode: " + e.getMessage());
		}
	}

}