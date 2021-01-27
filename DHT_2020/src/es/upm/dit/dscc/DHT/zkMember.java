package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	private static String pathTablas = "/tableDHT";
	private String myId;
	private String localAddress;
	//Variables de ViewManager;
	private int       nServersMax;
	private int       nServers;
	private int       nReplicas;
	private int       mutex;
	private List<String> previous    = null;
	private boolean   isQuorum       = false;
	private boolean   firstQuorum    = false;
	private boolean   pendingReplica = false;
	private String    failedServerTODO;
	private TableManager tableManager;
	private DHTUserInterface dht;
	
	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	private ZooKeeper zk;
	
	public zkMember(int nServersMax, int nReplicas, operationBlocking mutex, 
			TableManager tableManager, DHTUserInterface dht) {
		this.nServers     = 0;
		this.nServersMax  = nServersMax;
		this.nReplicas    = nReplicas;
		this.mutex        = -1;
		this.tableManager = tableManager;
		this.dht          = dht;
		
		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		createSessionZK(i);
		List<String> list = null;
		list = confZK(list);
		manageN(list);
		guardarInfoInTables();
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		ObjectOutputStream os;
		byte[] bytes= null;
		try {
			os = new ObjectOutputStream(bs);
			os.writeObject(tableManager.getDHTServers());
			os.close();
			bytes = bs.toByteArray();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Stat s = null;
		try {
			s = zk.exists(rootMembers, false);
			zk.setData(rootMembers, bytes, s.getVersion());
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		guardarServers();
		System.out.println("Created znode nember id:"+ myId );
		System.out.println("Created cluster ZK with: " + list.size() + " members");
		printListMembers(list);
		// Add the process to the members, servers and tables in zookeeper
		
	}
	
	public void createSessionZK(int i) {
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
	}
	
	public List<String> confZK(List<String> list) {
		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create a members folder, if it is not created
				String response = new String();
				Stat s = zk.exists(rootMembers, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootMembers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}
				// Create a tables folder, if it is not created
				Stat s0 = zk.exists(pathTablas+"-0", false); //this);
				Stat s1 = zk.exists(pathTablas+"-1", false); //this);
				Stat s2 = zk.exists(pathTablas+"-2", false); //this);
				if(s0 == null && s1 == null && s2 == null) {
					zk.create(pathTablas+"-0", new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					zk.create(pathTablas+"-1", new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					zk.create(pathTablas+"-2", new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("Nodos de tablas creadas");
				}
				// Create a znode for registering as member and get my id
				myId = zk.create(rootMembers + aMember, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				myId = myId.replace(rootMembers + "/", "");
				list = zk.getChildren(rootMembers, watcherMember, s); //this, s);
				this.tableManager.setLocalAddress(myId);
				this.localAddress = myId;
				return list;
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return list;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
		return list;
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
					manageN(list);
					guardarServers();
					printListMembers(list);
					System. out .println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 7) Init 0) Exit");				
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
	
	public boolean manageN(List<String> newN) {
		String address = null;
		// There are enough servers: nServers = nServersMax
		if (newN.size() > nServersMax) return false;
			// TODO: Handle if on servers fails before creating the first quorum
			// TODO: Currently supports one failure. Check if there are more than 1 fail
			//       Supposed that no server fails until there are quorum
		if (previous != null && newN.size() < previous.size()) {
			LOGGER.warning("A server has failed. There is no quorum!!!!!");
			// A server has failed
			String failedServer = crashedServer(previous, newN);
			deleteServer(failedServer);
			nServers--;
			isQuorum       = false;
			pendingReplica = true;
			previous       = newN;
			return false;
		} else {
			if (newN.size() > nServers) {
				if (nServers == 0 && newN.size()>0) {
					for (Iterator<String> iterator = newN.iterator(); iterator.hasNext();) {
						String itAddress = (String) iterator.next();
						addServer(itAddress);
						LOGGER.fine("Added a server. NServers: " + nServers +  
								"Server: " + itAddress + ".");
						if (nServers == nServersMax) {
							isQuorum    = true;
							firstQuorum = true;
						}
					}
				} else {
						HashMap<Integer, String> DHTServers;
						address = newN.get(newN.size() - 1);
						addServer(address);
						LOGGER.fine("Added a server. NServers: " + nServers 
								+ ". Server: " + address);
						if (nServers == nServersMax) {
							isQuorum    = true;
							// A server crashed and is a new one
							if (firstQuorum) {
								// A previous quorum existed. Then tolerate the fail
								// Add the new one in the DHTServer							
								String failedServer = newServer(newN);
								failedServerTODO     = failedServer;
								// Add the server in DHTServer
								DHTServers = addServer(failedServer);
								if (DHTServers == null) {
									LOGGER.warning("DHTServers is null!!");
								}
								// Send the Replicas 
								//transferData(failedServer);
								pendingReplica = true;
							} else {
								firstQuorum = true;
							}
						}
				}
			}
			LOGGER.fine(tableManager.printDHTServers());
			previous = newN;
			return true;
			}
	}
	
	//En cada tabla van dos servidores
	public void guardarInfoInTables() {
		if(nServers == nServersMax) {
			HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();
			HashMap<Integer, DHTUserInterface> tabla0 = obtenerTablas(pathTablas+"-0");
			HashMap<Integer, DHTUserInterface> tabla1 = obtenerTablas(pathTablas+"-1");
			HashMap<Integer, DHTUserInterface> tabla2 = obtenerTablas(pathTablas+"-2");
			if(tabla2 == null) {
				tabla2 = new HashMap<Integer, DHTUserInterface>();
			} else {
				switch (tableManager.getPosicion(myId)) {
				case 0:
					DHTTables.put(0, tabla0.get(0));// Al servidor 0 le metemos la tabla 0
					DHTTables.put(1, tabla1.get(1));// Al servidor 0 le metemos la tabla 1
					break;
				case 1:
					DHTTables.put(1, tabla1.get(1));// Al servidor 1 le metemos la tabla 1
					DHTTables.put(2, tabla2.get(2));// Al servidor 1 le metemos la tabla 2
					break;
				default:
					DHTTables.put(2, tabla2.get(2));// Al servidor 2 le metemos la tabla 2
					DHTTables.put(0, tabla0.get(0));// Al servidor 2 le metemos la tabla 0
					break;
				}
			}
		}
	}
	
	public HashMap<Integer, DHTUserInterface> obtenerTablas(String path){
		HashMap<Integer, DHTUserInterface> tabla = new HashMap<Integer, DHTUserInterface>();
		Stat s;
		byte[] bytes = null;
		try {
			s = zk.exists(pathTablas+"-1", false);
			bytes = zk.getData(pathTablas+"-1", false, s);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tabla = deserialize(bytes);
		return tabla;
	}
	
	public void guardarServers() {
		switch (tableManager.getPosicion(myId)) {
		case 0:
			actualizarTablas(pathTablas+"-0", tableManager.getDHTTables());
			break;
		case 1:
			actualizarTablas(pathTablas+"-1", tableManager.getDHTTables());
			break;
		default:
			actualizarTablas(pathTablas+"-2", tableManager.getDHTTables());
			break;
		}
	}
		
	public void actualizarTablas(String path, HashMap<Integer, DHTUserInterface> tabla){
		byte[] bytes = serialize(tabla);
		try {
			Stat s = zk.exists(path, false);
			zk.setData(path, bytes, s.getVersion());
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static byte[] serialize(HashMap<Integer, DHTUserInterface> tabla) {
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(bs);
			os.writeObject(tabla);
			os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		byte[] bytes = bs.toByteArray();
		return bytes;
	}
	
	public static HashMap<Integer, DHTUserInterface> deserialize(byte[] bytes) {
		ByteArrayInputStream bs = new ByteArrayInputStream(bytes);
		ObjectInputStream is;
		try {
			is = new ObjectInputStream(bs);
			HashMap<Integer, DHTUserInterface> tabla = (HashMap<Integer, DHTUserInterface>) is.readObject();
			return tabla;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
}

