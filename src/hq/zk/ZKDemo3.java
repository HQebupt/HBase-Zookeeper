package hq.zk;

import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * 3. 使用同步API创建、删除一个ZNode节点
 */
public class ZKDemo3 {
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

		String path = zk.create("/hq", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL); // 创建znode
		System.out.println(path + " is created.");
		zk.delete(path, -1); // 删除 znode
		zk.close();
	}

}
