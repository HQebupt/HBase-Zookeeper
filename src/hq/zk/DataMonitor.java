package hq.zk;

/**
 * A simple class that monitors the data and existence of a ZooKeeper
 * node. It uses asynchronous ZooKeeper APIs.
 */
import java.util.Arrays;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class DataMonitor implements Watcher, StatCallback {

	ZooKeeper zk;
	String znode;
	Watcher chainedWatcher; // 本例子是null,链式watcher，这个watcher收到通知后，通知别的Wathcer。自定义，复杂场景。
	boolean dead;
	DataMonitorListener listener;
	byte prevData[];

	public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher,
			DataMonitorListener listener) {
		this.zk = zk;
		this.znode = znode;
		this.chainedWatcher = chainedWatcher;
		this.listener = listener;
		// Get things started by checking if the node exists. We are going
		// to be completely event driven
		zk.exists(znode, true, this, null); //第二个参数是否设置watcher,一次性的
	}

	/**
	 * Other classes use the DataMonitor by implementing this method
	 */
	public interface DataMonitorListener {
		/**
		 * The existence status of the node has changed.
		 */
		void exists(byte data[]);

		/**
		 * The ZooKeeper session is no longer valid.
		 * 
		 * @param rc
		 *            the ZooKeeper reason code
		 */
		void closing(int rc);
	}

	public void process(WatchedEvent event) {
		System.out.println("DataMonitor收到事件通知：" + event.getState() + "\n");
		String path = event.getPath();
		if (event.getType() == Event.EventType.None) {
			// We are are being told that the state of the
			// connection has changed
			switch (event.getState()) {
			case SyncConnected:
				// In this particular example we don't need to do anything
				// here - watches are automatically re-registered with
				// server and any watches triggered while the client was
				// disconnected will be delivered (in order of course)
				break;
			case Expired:
				// It's all over
				dead = true;
				listener.closing(KeeperException.Code.SessionExpired);
				break;
			}
		} else {
			System.out.println("node改变了");
			if (path != null && path.equals(znode)) {
				// Something has changed on the node, let's find out
				zk.exists(znode, true, this, null);// 异步调用,再次设置watcher才能够继续监听这个节点。这个watcher是在zk初始化的时候指定。
			}
		}
		if (chainedWatcher != null) {
			chainedWatcher.process(event);
		}
	}

	public void processResult(int rc, String path, Object ctx, Stat stat) {
		boolean exists; // 若znode存在，后面的结果就是true,不存在就是false.
		switch (rc) {
		case Code.Ok:
			exists = true;
			break;
		case Code.NoNode:
			exists = false;
			break;
		case Code.SessionExpired:
		case Code.NoAuth:
			dead = true;
			listener.closing(rc);
			return;
		default:
			// Retry errors
			zk.exists(znode, true, this, null);
			return;
		}
		
		byte b[] = null;
		if (exists) {
			try {
				b = zk.getData(znode, false, null);
			} catch (KeeperException e) {
				// We don't need to worry about recovering now. The watch
				// callbacks will kick off any exception handling
				e.printStackTrace();
			} catch (InterruptedException e) {
				return;
			}
		}
		// 只要数据发生变化，就调用客户端listener的代码，将数据写入到file。
		if ((b == null && b != prevData)
				|| (b != null && !Arrays.equals(prevData, b))) {
			listener.exists(b);
			prevData = b;
		}
	}
}
