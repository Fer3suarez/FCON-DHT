package es.upm.dit.dscc.DHT;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class zkOperation {
	
	private static final int SESSION_TIMEOUT = 5000;
	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private static String rootOp = "/Op";
	private static String aOp = "/op-";
	private String myOp;
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	private ZooKeeper zk;
	
	public zkOperation(byte[] bytes){
		
		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				try {
					// Wait for creating the session. Use the object lock
					wait();
					//zk.exists("/",false);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} catch (Exception e) {
			System.out.println("Error");
		}
		// Add the process to the members, servers and tables in zookeeper
		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create a members folder, if it is not created
				String response = new String();
				Stat s = zk.exists(rootOp, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootOp, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}

				// Create a znode for registering as member and get my id
				myOp = zk.create(rootOp + aOp, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				myOp = myOp.replace(rootOp + "/", "");

				List<String> list = zk.getChildren(rootOp, false, s); //this, s);
				System.out.println("Created znode operation id:"+ myOp);
				printListMembers(list);
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
	}
	
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}
	
	private void printListMembers (List<String> list) {
		System.out.println("Remaining # operations:" + list.size());
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}
	
	
//-----------------------------------WATCHERS--------------------------------------------
		
		// Notified when the session is created
		private Watcher cWatcher = new Watcher() {
			public void process (WatchedEvent e) {
				System.out.println("Created session");
				notify();
			}
		};

		// Notified when the number of children in /member is updated
		private Watcher  watcherMember = new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("------------------Watcher Member------------------\n");		
				try {
					System.out.println("        Update!!");
					List<String> list = zk.getChildren(rootOp,  watcherMember); //this);
					printListMembers(list);
				} catch (Exception e) {
					System.out.println("Exception: wacherMember");
				}
			}
		};

}
