package hq.zk;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * 4. 使用异步API创建、删除一个ZNode节点
 */
public class ZKDemo4 {
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

		// 创建ZNode
		zk.create("/hq", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT, // 顺序节点会自动加上后缀ID
				new AsyncCallback.StringCallback() {
					@Override
					public void processResult(int rc, String path, Object ctx,
							String name) {
						// 服务器响应吗
						System.out.println("Create ResultCode: " + rc);
						// 接口调用时传入API的数据节点的路径参数值
						System.out.println("Create Znode: " + path);
						// 接口调用时传入API的ctx参数值
						System.out.println("Create Context: " + (String) ctx);
						// 实际在服务端创建的节点名
						System.out.println("Create Real Path: " + name);
					}
				}, "The Context");
		
		// 删除ZNode
		System.out.println("\n由于异步，使得这条语句提前输出了。。。。");
		zk.delete("/hq", -1, new AsyncCallback.VoidCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx) {
				System.out.println("Delete ResultCode: " + rc);
				System.out.println("Delete Znode: " + path);
				System.out.println("Delete Context: " + (String) ctx);
			}
		}, "The Context");

		zk.close();
	}

}
