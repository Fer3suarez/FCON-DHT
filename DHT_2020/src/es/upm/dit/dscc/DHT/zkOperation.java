package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	private static String rootOp = "/Ops";
	private static String aOp = "/op-";
	private String myOp;
	private operationBlocking mutex;
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
				myOp = zk.create(rootOp + aOp, bytes, 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				Stat s2 = zk.exists(myOp, false);
				zk.getData(myOp, watcherOperacion, s2);
				myOp = myOp.replace(rootOp + "/", "");
				List<String> list = zk.getChildren(rootOp, false, s); //this, s);
				System.out.println("Created znode operation id: "+ myOp);
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
	
	private void comprobarRespuestas(Operacion op, Stat s) {
		boolean opOk = false;
		int[] respuestas = op.getRespuestas();
		if(respuestas[0] != 0 && respuestas[1] != 0) opOk = true;
		if(opOk) {
			System.out.println("Se han recibido las dos respuestas y se puede borrar la operacion");
			//mutex.receiveOperation(op.getOperacion());
			try {
				zk.delete(myOp, s.getVersion());
			} catch (InterruptedException | KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

//------------------------------SERIALIZACIONES------------------------------------------
	public static byte[] serialize(Operacion op) {
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
	
	public static Operacion deserialize(byte[] bytes) {
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
	
//-----------------------------------WATCHERS--------------------------------------------
		
	// Notified when the session is created
	private Watcher cWatcher = new Watcher() {
		public void process (WatchedEvent e) {
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
	
	private Watcher watcherOperacion = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Operacion------------------\n");	
			try {
				Stat s = zk.exists(myOp, false);
				byte[] bytes = zk.getData(myOp, false, s);
				Operacion op;
				op = deserialize(bytes);
				System.out.println("La operacion es: "+ op);
				System.out.println("Se necesitan 2 respuestas para borrar la operacion");
				comprobarRespuestas(op, s);
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	};

}
