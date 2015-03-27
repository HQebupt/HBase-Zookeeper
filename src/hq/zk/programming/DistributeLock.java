package hq.zk.programming;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DistributeLock implements Watcher {

	static ZooKeeper zk = null;
	static Integer mutex = null;
	String name = null;
	String path = null;

	@Override
	synchronized public void process(WatchedEvent event) {
		System.out.println("process event:" + event.getType());
		synchronized (mutex) {
			mutex.notify(); // 通知一个醒来就好，避免羊群效应
		}
	}

	DistributeLock(String address) {
		if (zk == null) {
			try {
				System.out.println("Starting ZK:");
				zk = new ZooKeeper(address, 3000, this);
				mutex = new Integer(-1);
				System.out.println("Finished starting ZK: " + zk);
			} catch (IOException e) {
				System.out.println(e.toString());
				zk = null;
			}
		}
		// My node name
		try {
			name = InetAddress.getLocalHost().getCanonicalHostName().toString();
			System.out.println("name:" + name);
		} catch (UnknownHostException e) {
			System.out.println(e.toString());
		}
	}

	private String minSeq(List<String> list) {
		int min = Integer.parseInt(list.get(0).substring(11));
		String minString = list.get(0).substring(11);
		for (int i = 1; i < list.size(); i++) {
			if (min > Integer.parseInt(list.get(i).substring(11))) {
				min = Integer.parseInt(list.get(i).substring(11));
				minString = list.get(i).substring(11);
			}
		}
		return minString;
	}

	boolean getLock(String root) throws KeeperException, InterruptedException {
		// Create ZK node name
		if (zk != null) {
			try {
				Stat s = zk.exists(root, false);
				if (s == null) {
					zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
			} catch (KeeperException e) {
				System.out
						.println("Keeper exception when instantiating queue: "
								+ e.toString());
			} catch (InterruptedException e) {
				System.out.println("Interrupted exception");
			}
		}

		// create方法返回新建的节点的完整路径
		path = zk.create(root + "/" + name + "-", new byte[0],
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		String minStr;
		while (true) {
			synchronized (mutex) {
				List<String> list = zk.getChildren(root, true);
				minStr = minSeq(list);
				// 如果刚建的节点是根节点的所有子节点中序号最小的，则获得了锁，可以返回true
				if (Integer.parseInt(minStr) == Integer.parseInt(path
						.substring(17))) {
					return true;
				} else {
					mutex.wait(); // 等待事件（新建节点或删除节点）发生
					while (true) {
						zk.getChildren(root, true); // 必须再注册watcher，以便监听znode节点。
						Stat s = zk.exists(root + "/" + name + "-" + minStr,
								true); // 查看序号最小的子节点还在不在
						if (s != null) // 如果还在，则继续等待事件发生
							mutex.wait();
						else
							// 如果不在，则跳外层循环中，查看新的最小序号的子节点是谁
							break;
					}
				}
			}
		}
	}

	boolean releaseLock() throws KeeperException, InterruptedException {
		if (path != null) {
			zk.delete(path, -1);
			path = null;
		}
		return true;
	}

	public static void main(String[] args) throws KeeperException,
			InterruptedException {
		final String address = "eb177:2181,eb178:2181,eb179:2181";
		final String root = "/lock";
		DistributeLock lock1 = new DistributeLock(address);
		// 阻塞在getLock()
		if (lock1.getLock(root)) {
			System.out.println("T1 Get lock at " + System.currentTimeMillis());
			for (int i = 0; i < 10; ++i)
				Thread.sleep(500);
			lock1.releaseLock();
		}
		if (lock1.getLock(root)) {
			System.out.println("T2 Get lock at " + System.currentTimeMillis());
			lock1.releaseLock();
		}
	}

}
