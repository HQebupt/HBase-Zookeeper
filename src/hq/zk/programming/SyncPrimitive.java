package hq.zk.programming;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class SyncPrimitive implements Watcher {

	static ZooKeeper zk = null;
	static Integer mutex;

	String root;

	SyncPrimitive(String address) {
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
	}

	synchronized public void process(WatchedEvent event) {
		synchronized (mutex) {
			System.out.println("Process: " + event.getType());
			mutex.notify();
		}
	}

	/**
	 * Barrier
	 */
	static public class Barrier extends SyncPrimitive {
		int size;
		String name;

		/**
		 * Barrier constructor
		 * 
		 * @param address
		 * @param root
		 * @param size
		 */
		Barrier(String address, String root, int size) {
			super(address);
			this.root = root;
			this.size = size;

			// Create barrier node
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

			// My node name
			try {
				name = InetAddress.getLocalHost().getCanonicalHostName()
						.toString();
				System.out.println("name:" + name);
			} catch (UnknownHostException e) {
				System.out.println(e.toString());
			}
		}

		/**
		 * Join barrier
		 * 
		 * @return
		 * @throws KeeperException
		 * @throws InterruptedException
		 */

		boolean enter() throws KeeperException, InterruptedException {
			zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			while (true) {
				synchronized (mutex) {
					List<String> list = zk.getChildren(root, true);

					if (list.size() < size) {
						mutex.wait();
					} else {
						return true;
					}
				}
			}
		}

		/**
		 * Wait until all reach barrier
		 * 
		 * @return
		 * @throws KeeperException
		 * @throws InterruptedException
		 */

		boolean leave() throws KeeperException, InterruptedException {
			zk.delete(root + "/" + name, 0);
			while (true) {
				synchronized (mutex) {
					List<String> list = zk.getChildren(root, true);
					if (list.size() > 0) {
						mutex.wait();
					} else {
						return true;
					}
				}
			}
		}
	}

	/**
	 * Producer-Consumer queue
	 */
	static public class Queue extends SyncPrimitive {

		/**
		 * Constructor of producer-consumer queue
		 * 
		 * @param address
		 * @param name
		 */
		Queue(String address, String name) {
			super(address);
			this.root = name;
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
		}

		/**
		 * Add element to the queue.
		 * 
		 * @param i
		 * @return
		 */

		boolean produce(int i) throws KeeperException, InterruptedException {
			ByteBuffer b = ByteBuffer.allocate(4);
			byte[] value;

			// Add child with value i
			b.putInt(i);
			value = b.array();
			zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT_SEQUENTIAL);
			return true;
		}

		/**
		 * Remove first element from the queue.
		 * 
		 * @return return data value
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		int consume() throws KeeperException, InterruptedException {
			int retvalue = -1;
			Stat stat = null;

			// Get the first element available
			while (true) {
				synchronized (mutex) {
					List<String> list = zk.getChildren(root, true);
					if (list.size() == 0) {
						System.out.println("Going to wait");
						mutex.wait();
					} else {
						String suffixMin = list.get(0).substring(7);
						Integer min = new Integer(suffixMin);
						for (String s : list) {
							String suffix = s.substring(7);
							Integer tempValue = new Integer(suffix);
							System.out.println("Temporary value: " + tempValue);
							if (tempValue < min){
								min = tempValue;
								suffixMin = suffix;
							}
						}
						System.out.println("Temporary value: " + root
								+ "/element" + min);
						byte[] b = zk.getData(root + "/element" + suffixMin, false,
								stat);
						zk.delete(root + "/element" + suffixMin, 0);
						ByteBuffer buffer = ByteBuffer.wrap(b);
						retvalue = buffer.getInt();

						return retvalue;
					}
				}
			}
		}
	}

	public static void main(String args[]) {
		final String address = "eb177:2181,eb178:2181,eb179:2181";
		boolean queuetest = true;
		String[] para = { "", address, "3", "p" };
		if (queuetest)
			queueTest(para);
		else
			barrierTest(para);

	}

	public static void queueTest(String args[]) {
		Queue q = new Queue(args[1], "/app1");

		System.out.println("Input: " + args[1]);
		int i;
		Integer max = new Integer(args[2]);

		if (args[3].equals("p")) {
			System.out.println("Producer");
			for (i = 0; i < max; i++)
				try {
					q.produce(10 + i);
				} catch (KeeperException e) {
					System.out.println(e.toString());
				} catch (InterruptedException e) {
					System.out.println(e.toString());
				}
		} else {
			System.out.println("Consumer");

			for (i = 0; i < max; i++) {
				try {
					int data = q.consume();
					System.out.println("Item data: " + data);
				} catch (KeeperException e) {
					i--;
					System.out.println(e.toString());
				} catch (InterruptedException e) {
					System.out.println(e.toString());
				}
			}
		}
	}

	public static void barrierTest(String args[]) {
		Barrier b = new Barrier(args[1], "/b1", new Integer(args[2]));

		// 所有的线程都到达barrier后才能进行后续的计算
		try {
			boolean flag = b.enter();
			System.out.println("Entered barrier: " + args[2]);
			if (!flag)
				System.out.println("Error when entering the barrier");
		} catch (KeeperException e) {
			System.out.println(e.toString());
		} catch (InterruptedException e) {
			System.out.println(e.toString());
		}

		// 比如赛马比赛中， 等赛马陆续来到起跑线前。 一声令下，所有的赛马都飞奔而出。 下面的代码就是所有的赛马都飞奔而出
		// Generate random integer
		Random rand = new Random();
		int r = rand.nextInt(100);
		// Loop for rand iterations
		for (int i = 0; i < r; i++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				System.out.println(e.toString());
			}
		}

		// 所有的线程都完成自己的计算后才能离开barrier
		try {
			b.leave();
		} catch (KeeperException e) {
			System.out.println("here:" + e.toString());
			// 这个Exception总会发生，不知道为什么
		} catch (InterruptedException e) {
			System.out.println(e.toString());
		}
		System.out.println("Left barrier");
	}
}
