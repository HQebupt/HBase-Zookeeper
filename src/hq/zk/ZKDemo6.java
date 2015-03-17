package hq.zk;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 6.使用异步API获取子节点列表
 */
public class ZKDemo6 {
	private static final String CONNECTION_STRING = "eb177:2181,eb178:2181,eb179:2181";
	
	public static void main(String[] args) throws Exception {

		final CountDownLatch connectedSignal = new CountDownLatch(1);
		final ZooKeeper zk = new ZooKeeper(CONNECTION_STRING, 5000, null);
		zk.register(new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("收到事件通知：" + event.getState() + "\n");
				if (event.getState() == KeeperState.SyncConnected) {
					if (event.getType() == EventType.None
							&& event.getPath() == null) {
						connectedSignal.countDown();
					} else if (event.getType() == EventType.NodeChildrenChanged) {
						try {
							System.out.println("NodeChildrenChanged.");
							List<String> children = zk.getChildren(
									event.getPath(), true);
							System.out.println("Children: " + children);
						} catch (KeeperException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		});
		connectedSignal.await();

		zk.create("/hq", "root".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		zk.create("/hq/node1", "node1".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		zk.create("/hq/node2", "node2".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);

		zk.getChildren("/hq", true, new AsyncCallback.Children2Callback() {
			@Override
			public void processResult(int rc, String path, Object ctx,
					List<String> children, Stat stat) {
				System.out.println("ResultCode: " + rc);
				System.out.println("ZNode: " + path);
				System.out.println("Context: " + ctx);
				System.out.println("Children: " + children);
				System.out.println("Stat: " + stat);
			}
		}, "The Context");

		zk.create("/hq/node3", "node3".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);

		Thread.sleep(10 * 1000);
		zk.close();
	}

}
