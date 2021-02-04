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
	private static java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private static String rootMembers = "/members";
	private static String aMember = "/member-";
	private static String pathTablas = "/tableDHT";
	private static String rootOp = "/Ops";
	private static String aOp = "/op-";
	private static String myId;
	private static String myOp;
	private String localAddress;
	private static operationBlocking mutex;
	//Variables de ViewManager;
	private static int       nServersMax;
	private static int       nServers;
	private int       nReplicas;
	private List<String> previousServer    = null;
	private boolean   isQuorum       = false;
	private boolean   firstQuorum    = false;
	private boolean   pendingReplica = false;
	private String    failedServerTODO;
	private static TableManager tableManager;
	private static DHTUserInterface dht;
	
	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	private static ZooKeeper zk;
	
	public zkMember(int nServersMax, int nReplicas, operationBlocking mutex, 
			TableManager tableManager, DHTUserInterface dht) {
		zkMember.nServers     = 0;
		zkMember.mutex        = mutex;
		zkMember.nServersMax  = nServersMax;
		this.nReplicas    = nReplicas;
		zkMember.tableManager = tableManager;
		zkMember.dht          = dht;
		
		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		createSessionZK(i);
		List<String> list = null;
		list = confZK(list);
		Collections.sort(list);
		manageServers(list);
		actualizarServers();
		guardarDatosEnTablas();
		Stat s2;
		try {
			s2 = zk.exists(rootOp, false);
			if(s2 == null) {
				myOp = zk.create(rootOp, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				System.out.println("ZNode de operaciones creado");
				myOp = myOp.replace(rootOp + "/", "");
			}
			s2 = zk.exists(rootOp, false);
			List<String> listOperaciones = zk.getChildren(rootOp, watcherOperacion, s2);
			Collections.sort(list);
			System.out.println("Operations: " + listOperaciones.size());
			printListOperaciones(listOperaciones);
		} catch (KeeperException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
	}
	
	public void createSessionZK(int i) {
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				try {
					wait();
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
			try {
				// Create a members folder, if it is not created
				Stat s = zk.exists(rootMembers, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					zk.create(rootMembers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("ZNode de miembros creado");
				}
				// Create a tables folder, if it is not created
				Stat s0 = zk.exists(pathTablas+"-0", false); //this);
				Stat s1 = zk.exists(pathTablas+"-1", false); //this);
				Stat s2 = zk.exists(pathTablas+"-2", false); //this);
				if(s0 == null && s1 == null && s2 == null) {
					// Created the znodes, if it is not created.
					zk.create(pathTablas+"-0", new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					zk.create(pathTablas+"-1", new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					zk.create(pathTablas+"-2", new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("ZNodes de tablas creadas");
				}
				//Create a znode for registering as member and get my id
				myId = zk.create(rootMembers + aMember, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				myId = myId.replace(rootMembers + "/", "");
				list = zk.getChildren(rootMembers, watcherMember, s); //this, s);
				zkMember.tableManager.setLocalAddress(myId);
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
		Collections.sort(list);
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String string = iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}
	
	private static void printListOperaciones (List<String> list) {
		System.out.println("Remaining # operaciones:" + list.size());
		Collections.sort(list);
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String string = iterator.next();
			System.out.print(string + ", ");
		}
		System.out.println();
	}
//-----------------------------------------------------------------------------------------------------------------------------
//-----------------------------------------------------WATCHERS----------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------------

		// Notified when the session is created
		private static Watcher cWatcher = new Watcher() {
			@Override
			public void process (WatchedEvent e) {
				System.out.println("Created session");
				notify();
			}
		};

		// Notified when the number of children in /member is updated
		private Watcher  watcherMember = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("------------------Watcher Member------------------\n");		
				try {
					System.out.println("        Update!!");
					List<String> list = zk.getChildren(rootMembers,  watcherMember); //this);
					Collections.sort(list);
					manageServers(list);
					actualizarServers();
					printListMembers(list);
					System.out.println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 7) Init 0) Exit");				
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		};
		
		private static Watcher  watcherOperacion = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("------------------Watcher Operacion------------------\n");		
				try {
					List<String> list = zk.getChildren(rootOp,  watcherOperacion); //this);
					Collections.sort(list);
					if(list.size() > 0) {
						printListOperaciones(list);
						procesarOperacion();
						Stat s = zk.exists(rootOp, false);
						zk.getChildren(rootOp, watcherOperacion, s);
					} else {
						//actualizarServers();
						guardarDatosEnTablas();
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
	public static void crearZnodeOperacion(byte[] bytes) {
		String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);
		if (zk != null) {
			try {
				String response = new String();
				Stat s = zk.exists(rootOp, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootOp, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}
				// Create a znode for registering as member and get my id
				myOp = zk.create(rootOp + aOp, bytes, 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				myOp = myOp.replace(rootOp + "/", "");
				List<String> list = zk.getChildren(rootOp, false, s); //this, s);
				Collections.sort(list);
				System.out.println("Created znode operation id: "+ myOp);
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
	}
	
	public static void procesarOperacion() {
		List<String> list;
		boolean operar = false;
		try {
			list = zk.getChildren(rootOp, false);
			Collections.sort(list);
			myOp = list.get(0);
			if(list.indexOf(myOp.substring(myOp.lastIndexOf('/')+1)) == 0) {
				LOGGER.fine("Puedo procesar la operacion");
				Stat s = zk.exists(rootOp, false);
				byte[] bytes = zk.getData(rootOp+"/"+myOp, false, s);
				Operacion op = deserializeOp(bytes);
				int[] nodos = op.getNodos();
				for (int j = 0; j < nodos.length; j++) {
					if(nodos[j] == tableManager.getPosicion(myId)) {
						LOGGER.fine("El servidor necesita procesar la operacion y dar una respuesta");
						operar = true;
					}
				}
				if(operar) { //Necesito devolver la operacion y la respuesta
					OperationsDHT o = op.getOperacion();
					int[] respuestas;
					int respuesta;
					switch ((OperationEnum) o.getOperation()) {
					case GET_MAP:
						respuesta = dht.getMsg(o.getKey());
						o.setValue(respuesta);
						break;
					case PUT_MAP:
						respuesta = dht.putMsg(o.getMap());
						o.setValue(respuesta);
						break;
					default:
						respuesta = dht.removeMsg(o.getKey());
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
					byte [] data = serializeOp(op);
					s = zk.exists(rootOp+"/"+myOp, false);
					if(s!=null) zk.setData(rootOp+"/"+myOp, data, s.getVersion());
				}
				if(!operar) LOGGER.fine("Este servidor no procesa esta operacion");
				
				Stat s3 = zk.exists(rootOp+"/"+myOp, false);
				byte[] bytes3;
				if(rootOp+"/"+myOp == "") bytes3 = null;
					bytes3 = zk.getData(rootOp+"/"+myOp, false, s3);
				Operacion ope;
				ope = deserializeOp(bytes3);
				LOGGER.fine("La operacion es: "+ op);
				LOGGER.fine("Se necesitan 2 respuestas para borrar la operacion");
				comprobarRespuestas(ope, s3);
				System.out.println("Operación terminada");
				
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void comprobarRespuestas(Operacion op, Stat s) {
		boolean opOk = false;
		int[] respuestas = op.getRespuestas();
		boolean eliminar = false;
		if(respuestas[0] != 0 || respuestas[1] != 0) opOk = eliminar = true;
		if(opOk && eliminar) {
			LOGGER.fine("Se ha recibido la respuesta y se puede borrar la operacion");
			LOGGER.fine("Se ha eliminado el znode de la operacion: "+ myOp);
			mutex.receiveOperation(op.getOperacion());
			List<String> listOperaciones ;
			try {
				s = zk.exists(rootOp+"/"+myOp, false);
				if(s != null) zk.delete(rootOp+"/"+myOp, s.getVersion());
				listOperaciones = zk.getChildren(rootOp, watcherOperacion, s);
				System.out.println("Operations: " + listOperaciones.size());
				printListOperaciones(listOperaciones);
			} catch (InterruptedException | KeeperException e) {
				// TODO Auto-generated catch block
				try {
					listOperaciones = zk.getChildren(rootOp, watcherOperacion, s);
					System.out.println("Operations: " + listOperaciones.size());
					printListOperaciones(listOperaciones);
				} catch (KeeperException | InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		//actualizarServers();
		guardarDatosEnTablas();
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
					LOGGER.fine("Added a server. NServers: " + nServers);
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
						String itAddress = iterator.next();
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
	public static void guardarDatosEnTablas() {
		if(nServers == nServersMax) {
			HashMap<Integer, DHTUserInterface> DHTTables = tableManager.getDHTTables();
			HashMap<Integer, DHTUserInterface> tabla0 = obtenerTablas(pathTablas+"-0");
			HashMap<Integer, DHTUserInterface> tabla1 = obtenerTablas(pathTablas+"-1");
			HashMap<Integer, DHTUserInterface> tabla2 = obtenerTablas(pathTablas+"-2");
			//System.out.println("El myId antes de guardar datos es: "+ myId);
			switch (tableManager.getPosicion(myId)) {
			case 0:
				//DHTTables.put(0, tabla0.get(0));// Al servidor 0 le metemos la tabla 0
				DHTTables.put(1, tabla1.get(1));// Al servidor 0 le metemos la tabla 1
				break;
			case 1:
				//DHTTables.put(1, tabla1.get(1));// Al servidor 1 le metemos la tabla 1
				DHTTables.put(2, tabla2.get(2));// Al servidor 1 le metemos la tabla 2
				break;
			default:
				//DHTTables.put(2, tabla2.get(2));// Al servidor 2 le metemos la tabla 2
				DHTTables.put(0, tabla0.get(0));// Al servidor 2 le metemos la tabla 0
				break;
			}
		}
	}
	
	public static HashMap<Integer, DHTUserInterface> obtenerTablas(String path){
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
		if(bytes.length != 0) {
			tabla = deserialize(bytes);
		}
		return tabla;
	}
	
	public static void actualizarServers() {
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
		
	public static void actualizarTablas(String path, HashMap<Integer, DHTUserInterface> tabla){
		byte[] bytes = null;
		try {
			bytes = serialize(tabla);
			Stat s = zk.exists(path, false);
			if(s!=null) zk.setData(path, bytes, s.getVersion());
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

