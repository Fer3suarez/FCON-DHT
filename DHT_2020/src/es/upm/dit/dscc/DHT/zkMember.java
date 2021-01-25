package es.upm.dit.dscc.DHT;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class zkMember{
	private static final int SESSION_TIMEOUT = 5000;
	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private static String rootMembers = "/members";
	private static String aMember = "/member-";
	private static String pathTablas = "/tablesDHT";
	private static String pathServers = "/serversDHT";
	private String myId;
	private String localAddress;
	//Variables de ViewManager;
	private int       nReplica;
	private int       nServersMax;
	private int       nServers;
	private List<String> previous    = null;
	private boolean   isQuorum       = false;
	private boolean   firstQuorum    = false;
	private boolean   pendingReplica = false;
	private String    failedServerTODO;
	private TableManager tableManager;
	
	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	private ZooKeeper zk;
	
	public zkMember () {
		this.nServers = 0;
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
		// Add the process to the members in zookeeper
		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create a folder, if it is not created
				String response = new String();
				Stat s = zk.exists(rootMembers, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootMembers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}

				// Create a znode for registering as member and get my id
				myId = zk.create(rootMembers + aMember, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

				myId = myId.replace(rootMembers + "/", "");

				List<String> list = zk.getChildren(rootMembers, false, s); //this, s);
				System.out.println("Created znode nember id:"+ myId );
				printListMembers(list);
				//esLider(1);
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
	}
	
	public String getLocalAddress() {
		return this.localAddress;
	}
	
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}
	
	private void printListMembers (List<String> list) {
		System.out.println("Remaining # members:" + list.size());
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
					List<String> list = zk.getChildren(rootMembers,  watcherMember); //this);
					printListMembers(list);
				} catch (Exception e) {
					System.out.println("Exception: wacherMember");
				}
			}
		};
//------------------ANTIGUO VIEWMANAGER (GESTION DE SERVERS)-----------------------------------	
	
	public HashMap<Integer, String>  addServer(String address) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();

		if (nServers >= nServersMax) {
			return null;
		} else {
			// Find a hole
			for (int i = 0; i < nServersMax; i ++) {
				if (DHTServers.get(i) == null) {
					DHTServers.put(i, address);
					if (DHTTables.get(i) == null) {
						DHTTables.put(i, new DHTHashMap());
					}	
					nServers++;
					//sendMessages.sendServers(address, DHTServers);
					LOGGER.finest("Added a server. NServers: " + nServers);
					return DHTServers;
				}
			}
		}
		LOGGER.warning("Error: This sentence shound not run");
		return null;
	}
	
	public Integer deleteServer(String address) {
		HashMap<Integer, String> DHTServers = tableManager.getDHTServers();
		
		for (int i = 0; i < nServersMax; i ++) {
			if (address.equals(DHTServers.get(i))) {
				DHTServers.remove(i);
				return i;
			}
		}
		LOGGER.warning("This sentence should no be run");
		return null;
	}
	
	public String crashedServer(List<String> previousN, List<String> newN) {
		for (int k = 0; k < newN.size(); k++) {
			if (previousN.get(k).
					equals(newN.get(k))) {
			} else {
				return previousN.get(k);
			}
		}
		return previousN.get(previousN.size() - 1);
	}

	public String newServer(List<String> n) {
		return n.get(n.size() - 1);
	}
	
	public boolean isQuorun() {
		return isQuorum;
	}
	
//	public boolean manageNodes(List<String> newN) {
//		String address = null;
//		// There are enough servers: nServers = nServersMax
//		if (newN.size() > nServersMax) return false;
//		// TODO: Handle if on servers fails before creating the first quorum
//		// TODO: Currently supports one failure. Check if there are more than 1 fail
//		//       Supposed that no server fails until there are quorum
//		if (previous!= null && newN.size() < previous.size()) {
//			LOGGER.warning("A server has failed. There is no quorum!!!!!");
//			// A server has failed
//			String failedServer = crashedServer(previous, newN);
//			deleteServer(failedServer);
//			nServers--;
//			isQuorum       = false;
//			pendingReplica = true;
//			previous   = newN;
//			return false;
//		}
//		if (newN.size() > nServers) {
//			if (nServers == 0 && newN.size()>0) {
//				for (Iterator<String> iterator = newN.iterator(); iterator.hasNext();) {
//					String itAddress = (String) iterator.next();
//					addServer(itAddress);
//					LOGGER.fine("Added a server. NServers: " + nServers +  
//							"Server: " + itAddress + ".");
//					if (!itAddress.equals(localAddress)) {
//						sendMessages.sendInit(itAddress);
//					}
//					if (nServers == nServersMax) {
//						isQuorum    = true;
//						firstQuorum = true;
//					}
//				}
//			} else {
//				if (newN.size() > nServers) {
//					HashMap<Integer, String> DHTServers;
//					address = newN.get(newN.size() - 1);
//					addServer(address);
//					LOGGER.fine("Added a server. NServers: " + nServers 
//							+ ". Server: " + address);
//					if (nServers == nServersMax) {
//						isQuorum    = true;
//						// A server crashed and is a new one
//						if (firstQuorum) {
//							// A previous quorum existed. Then tolerate the fail
//							// Add the new one in the DHTServer							
//							String failedServer = newServer(newN);
//							failedServerTODO     = failedServer;
//							// Add the server in DHTServer
//							DHTServers = addServer(failedServer);
//							if (DHTServers == null) {
//								LOGGER.warning("DHTServers is null!!");
//							} else {
//								sendMessages.sendServers(failedServer, DHTServers);
//							}
//
//							pendingReplica = true;
//						} else {
//							firstQuorum = true;
//						}
//					}
//				}
//			}
//		}
//		LOGGER.fine(tableManager.printDHTServers());
//		previous = newN;
//		return true;
//	}
}
