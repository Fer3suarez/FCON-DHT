package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
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
	private static String pathOperaciones = "/Ops";
	private String myId;
	private String myOp;
	private String localAddress;
	//Variables de ViewManager;
	private int       nServersMax;
	private int       nServers;
	private int       nReplicas;
	private int       mutex;
	private List<String> previousServer    = null;
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
		manageServers(list);
		guardarInfoEnTablas();
		actualizarServers();
		confOperaciones();
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
				Stat s = zk.exists(rootMembers, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					zk.create(rootMembers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("Nodo de miembros creado");
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
				Collections.sort(list);
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
	
//-----------------------------------------------------------------------------------------------------------------------------
//-----------------------------------------------------WATCHERS----------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------------
		
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
					manageServers(list);
					actualizarServers();
					printListMembers(list);
					System.out.println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 7) Init 0) Exit");				
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		};
		
		private Watcher  watcherOperacion = new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("------------------Watcher Operacion------------------\n");		
				try {
					List<String> list = zk.getChildren(pathOperaciones,  watcherOperacion); //this);
					if(list.size() > 0) {
						printListMembers(list);
						procesarOperacion();				
					} else {
						actualizarServers();
						System.out.println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 7) Init 0) Exit");				
					}
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		};
		
//-----------------------------------------------------------------------------------------------------------------------------
//------------------------------------------------OPERACIONES------------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------------
	public void confOperaciones() {
		if(zk != null) {
			try {
				Stat s = zk.exists(pathOperaciones, false);
				if(s == null) {
					myOp = zk.create(pathOperaciones, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("Nodo de operaciones creado");
					myOp = myOp.replace(pathOperaciones + "/", "");
				}
//				myOp = zk.create(pathOperaciones+"/op-", new byte[0], 
//						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				myOp = zkOperation.getOpPath();
				Stat s2 = zk.exists(myOp, false);
				zk.getData(myOp, watcherOperacion, s2);
				s = zk.exists(pathOperaciones, false);
				List<String> listOperaciones = zk.getChildren(pathOperaciones, watcherOperacion, s);
				System.out.println("Operations: " + listOperaciones.size());
				printListMembers(listOperaciones);
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}
		
	public void procesarOperacion() {
		List<String> list;
		boolean operar = false;
		try {
			list = zk.getChildren(pathOperaciones, false);
			int indice = list.indexOf(myOp.substring(myOp.lastIndexOf('/')+1));
			if(indice == 0) {
				System.out.println("Soy el lider");
				Stat s = zk.exists(pathOperaciones, false);
				byte[] bytes = zk.getData(pathOperaciones, false, s);
				Operacion op = deserializeOp(bytes);
				int[] nodos = op.getNodos();
				for (int j = 0; j < nodos.length; j++) {
					if(nodos[j] == tableManager.getPosicion(myId)) {
						System.out.println("El servidor necesita procesar la operacion y dar una respuesta");
						operar = true;
					}
				}
				if(operar) { //Necesito devolver la operacion y la respuesta
					OperationsDHT o = op.getOperacion();
					int[] respuestas;
					int respuesta;
					switch ((OperationEnum) o.getOperation()) {
					case GET_MAP:
						respuesta = dht.get(o.getKey());
						o.setValue(respuesta);
						break;
					case PUT_MAP:
						respuesta = dht.put(o.getMap());
						o.setValue(respuesta);
						break;
					default:
						respuesta = dht.remove(o.getKey());
						o.setValue(respuesta);
						break;
					}
					respuestas = op.getRespuestas();
					for (int i = 0; i < respuestas.length; i++) {
						if(respuestas[i] == 0) {
							respuestas[i] = respuesta;
							break; //Metemos la respuesta y salimos
						}
					}
					op.setRespuestas(respuestas);
					op.setOperacion(o);
				}
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
//-----------------------------------------------------------------------------------------------------------------------------
//------------------------------------------ANTIGUO VIEWMANAGER (GESTION DE SERVERS)-------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------------
	
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
	
	public String crashedServer(List<String> previousServerN, List<String> newServer) {
		for (int k = 0; k < newServer.size(); k++) {
			if (previousServerN.get(k).
					equals(newServer.get(k))) {
			} else {
				return previousServerN.get(k);
			}
		}
		return previousServerN.get(previousServerN.size() - 1);
	}

	public String newServer(List<String> n) {
		return n.get(n.size() - 1);
	}
	
	public boolean isQuorun() {
		return isQuorum;
	}
	
	public boolean manageServers(List<String> newServer) {
		String address = null;
			// TODO: Handle if on servers fails before creating the first quorum
			// TODO: Currently supports one failure. Check if there are more than 1 fail
			//       Supposed that no server fails until there are quorum
		if (previousServer != null && newServer.size() < previousServer.size()) {
			LOGGER.warning("A server has failed. There is no quorum!!!!!");
			// A server has failed
			Collections.sort(newServer);
			String failedServer = crashedServer(previousServer, newServer);
			deleteServer(failedServer);
			nServers--;
			isQuorum       = false;
			pendingReplica = true;
			previousServer       = newServer;
			return false;
		} else {
			// There are enough servers: nServers = nServersMax
			if (newServer.size() > nServersMax) return false;
			if (newServer.size() > nServers) {
				if (nServers == 0 && newServer.size()>0) {
					for (Iterator<String> iterator = newServer.iterator(); iterator.hasNext();) {
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
						Collections.sort(newServer);//Ordenar para la actualización
						address = newServer.get(newServer.size() - 1);
						addServer(address);
						LOGGER.fine("Added a server. NServers: " + nServers 
								+ ". Server: " + address);
						if (nServers == nServersMax) {
							isQuorum    = true;
							// A server crashed and is a new one
							if (firstQuorum) {
								// A previousServer quorum existed. Then tolerate the fail
								// Add the new one in the DHTServer							
								String failedServer = newServer(newServer);
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
			previousServer = newServer;
			return true;
			}
	}
	
	//En cada tabla van dos servidores
	public void guardarInfoEnTablas() {
		if(nServers == nServersMax) {
			HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();
			HashMap<Integer, DHTUserInterface> tabla0 = obtenerTablas(pathTablas+"-0");
			HashMap<Integer, DHTUserInterface> tabla1 = obtenerTablas(pathTablas+"-1");
			HashMap<Integer, DHTUserInterface> tabla2 = obtenerTablas(pathTablas+"-2");
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
	
	public HashMap<Integer, DHTUserInterface> obtenerTablas(String path){
		HashMap<Integer, DHTUserInterface> tabla = new HashMap<Integer, DHTUserInterface>();
		Stat s;
		byte[] bytes = null;
		try {
			s = zk.exists(path, false);
			bytes = zk.getData(path, false, s);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tabla = deserialize(bytes);
		return tabla;
	}
	
	public void actualizarServers() {
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
		byte[] bytes = null;
		try {
			bytes = serialize(tabla);
			Stat s = zk.exists(path, false);
			zk.setData(path, bytes, s.getVersion());
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

//-----------------------------------------------------------------------------------------------------------------------------
//---------------------------------------------SERIALIZACION-------------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------------
	
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
	
	public static byte[] serializeOp(Operacion op) {
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(bs);
			os.writeObject(op);
			os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		byte[] bytes = bs.toByteArray();
		return bytes;
	}
	
	public static Operacion deserializeOp(byte[] bytes) {
		ByteArrayInputStream bs = new ByteArrayInputStream(bytes);
		ObjectInputStream is;
		try {
			is = new ObjectInputStream(bs);
			Operacion op = (Operacion) is.readObject();
			return op;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
}

