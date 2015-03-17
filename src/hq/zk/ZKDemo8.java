package hq.zk;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 8.使用异步API获取节点数据内容
 */
public class ZKDemo8 {
	private static final String CONNECTION_STRING = "eb177:2181,eb178:2181,eb179:2181";

	public static void main(String[] args) throws Exception {

		final CountDownLatch connectedSignal = new CountDownLatch(1);
		final ZooKeeper zk = new ZooKeeper(CONNECTION_STRING, 5000,
				new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						System.out.println("收到事件通知：" + event.getState() + "\n");
						if (event.getState() == KeeperState.SyncConnected) {
							if (event.getType() == EventType.None
									&& event.getPath() == null) {
								connectedSignal.countDown();
							}
						}
					}
				});
		connectedSignal.await();
		
		System.out.println("同步创建，异步获取数据。before");
		zk.create("/hq", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println("同步创建，异步获取数据。after");
		zk.getData("/hq", false, new AsyncCallback.DataCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx,
					byte[] data, Stat stat) {
				System.out.println("ResultCode: " + rc);
				System.out.println("ZNode: " + path);
				System.out.println("Context: " + ctx);
				System.out.println("NodeData: " + new String(data));
				System.out.println("Stat: " + stat);
			}
		}, "The Context");
		zk.delete("/hq", -1);
		zk.close();
	}

}
