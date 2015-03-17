package hq.zk;

/**
 * A simple example program to use DataMonitor to start and
 * stop executables based on a znode. The program watches the
 * specified znode and saves the data that corresponds to the
 * znode in the filesystem. It also starts the specified program
 * with the specified arguments when the znode exists and kills
 * the program if the znode goes away.
 */
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Executor implements Watcher, Runnable,
		DataMonitor.DataMonitorListener {
	private static final String CONNECTION_STRING = "eb177:2181,eb178:2181,eb179:2181";
	ZooKeeper zk;
	String znode;
	DataMonitor dm;
	String filename;
	String exec[];
	Process child;

	public Executor(String hostPort, String znode, String filename,
			String exec[]) throws KeeperException, IOException {
		this.filename = filename;
		this.exec = exec;
		zk = new ZooKeeper(hostPort, 3000, this);
		dm = new DataMonitor(zk, znode, null, this);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String hostPort = CONNECTION_STRING;
		String znode = "/hq";
		String filename = "file1";
		String exec[] = {"java"};
		// System.arraycopy(args, 3, exec, 0, exec.length);
		try {
			Thread t = new Thread(new Executor(hostPort, znode, filename, exec));
			t.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/***************************************************************************
	 * We do process any events ourselves, we just need to forward them on.
	 * 
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.proto.WatcherEvent)
	 */
	@Override
	public void process(WatchedEvent event) {
		System.out.println("Executor收到事件通知：" + event.getState() + "\n");
		dm.process(event);
	}

	@Override
	public void run() {
		try {
			synchronized (this) {
				while (!dm.dead) {
					wait();
				}
			}
			System.out.println("Executor 运行完毕.");
		} catch (InterruptedException e) {
		}
	}

	public void closing(int rc) { // 调用这个方法时,wait()解锁，此时dm.dead=true了，程序结束。
		synchronized (this) {
			notifyAll();
		}
	}

	static class StreamWriter extends Thread {
		OutputStream os;

		InputStream is;

		StreamWriter(InputStream is, OutputStream os) {
			this.is = is;
			this.os = os;
			start();
		}

		public void run() {
			byte b[] = new byte[80];
			int rc;
			try {
				while ((rc = is.read(b)) > 0) {
					os.write(b, 0, rc);
				}
			} catch (IOException e) {
			}
		}
	}

	/**
	 * data 为空就停止child进程
	 * data 不为空停止child进程，写入数据到filename，同时执行exec的进程
	 */
	public void exists(byte[] data) {
		if (data == null) {
			if (child != null) {
				System.out.println("Killing sub process");
				child.destroy(); // kill 它的所有子进程.
				try {
					child.waitFor(); // 等待子进程被kill
				} catch (InterruptedException e) {
				}
			}
			child = null;
		} else {
			if (child != null) {
				System.out.println("Stopping child Process");
				child.destroy();
				try {
					child.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			try {
				FileOutputStream fos = new FileOutputStream(filename);
				fos.write(data);
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				System.out.println("Starting child Process");
				child = Runtime.getRuntime().exec(exec);
				System.out.println("exec standard out：");
				new StreamWriter(child.getInputStream(), System.out); // exec "java"的输出
				System.out.println("exec standard err:");
				new StreamWriter(child.getErrorStream(), System.err);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
