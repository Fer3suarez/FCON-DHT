package es.upm.dit.dscc.DHT;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;


public class zkMember{
	private static final int SESSION_TIMEOUT = 5000;

	private static String rootMembers = "/members";
	private static String aMember = "/member-";
	private String myId;
	private String localAddress;
	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	//private ZooKeeper zk;
	
	public zkMember () {
//
//		// Select a random zookeeper server
//		Random rand = new Random();
//		int i = rand.nextInt(hosts.length);
//
//		// Create a session and wait until it is created.
//		// When is created, the watcher is notified
//		try {
//			if (zk == null) {
//				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
//				try {
//					// Wait for creating the session. Use the object lock
//					wait();
//					//zk.exists("/",false);
//				} catch (Exception e) {
//					// TODO: handle exception
//				}
//			}
//		} catch (Exception e) {
//			System.out.println("Error");
//		}
//
//		// Add the process to the members in zookeeper
//
//		if (zk != null) {
//			// Create a folder for members and include this process/server
//			try {
//				// Create a folder, if it is not created
//				String response = new String();
//				Stat s = zk.exists(rootMembers, false); //this);
//				if (s == null) {
//					// Created the znode, if it is not created.
//					response = zk.create(rootMembers, new byte[0], 
//							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//					System.out.println(response);
//				}
//
//				// Create a znode for registering as member and get my id
//				myId = zk.create(rootMembers + aMember, new byte[0], 
//						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
//
//				myId = myId.replace(rootMembers + "/", "");
//
//				List<String> list = zk.getChildren(rootMembers, false, s); //this, s);
//				System.out.println("Created znode nember id:"+ myId );
//				printListMembers(list);
//				//esLider(1);
//			} catch (KeeperException e) {
//				System.out.println("The session with Zookeeper failes. Closing");
//				return;
//			} catch (InterruptedException e) {
//				System.out.println("InterruptedException raised");
//			}
//
//		}
	}
	
	public String getLocalAddress() {
		return this.localAddress;
	}
//
//	// Notified when the session is created
//	private Watcher cWatcher = new Watcher() {
//		public void process (WatchedEvent e) {
//			System.out.println("Created session");
//			System.out.println(e.toString());
//			notify();
//		}
//	};
//
//	// Notified when the number of children in /member is updated
//	private Watcher  watcherMember = new Watcher() {
//		public void process(WatchedEvent event) {
//			System.out.println("------------------Watcher Member------------------\n");		
//			try {
//				System.out.println("        Update!!");
//				List<String> list = zk.getChildren(rootMembers,  watcherMember); //this);
//				printListMembers(list);
//			} catch (Exception e) {
//				System.out.println("Exception: wacherMember");
//			}
//		}
//	};
//	
//	private Watcher  watcherLock = new Watcher() { //Watcher para el cerrojo distribuido
//		public void process(WatchedEvent event) {
//			System.out.println("------------------Watcher Locker------------------\n");		
//			try {
//				System.out.println("        Update!!");
//				List<String> list = zk.getChildren(pathLock,  watcherLock); //this);
//				printListMembers(list);
//				esLider(1);
//			} catch (Exception e) {
//				System.out.println("Exception: wacherLocker");
//			}
//		}
//	};
//	
//	@Override
//	public void process(WatchedEvent event) {
//		try {
//			System.out.println("Unexpected invocated this method. Process of the object");
//			List<String> list = zk.getChildren(rootMembers, watcherLock); //this);
//			printListMembers(list);
//		} catch (Exception e) {
//			System.out.println("Unexpected exception. Process of the object");
//		}
//	}
//	
//	private void printListMembers (List<String> list) {
//		System.out.println("Remaining # members:" + list.size());
//		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
//			String string = (String) iterator.next();
//			System.out.print(string + ", ");				
//		}
//		System.out.println();
//	}
//	
}
