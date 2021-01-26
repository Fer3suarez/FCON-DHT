package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

public class DHTOperaciones implements DHTUserInterface {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private operationBlocking mutex;
	private int nReplicas;
	private TableManager      tableManager;

	public DHTOperaciones (
			operationBlocking mutex,
			TableManager tableManager,
			int nReplicas) {

		this.mutex        = mutex;
		this.tableManager = tableManager;
		this.nReplicas    = nReplicas;
	}
	
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

	@Override
	public Integer put(DHT_Map map) {
	
		OperationsDHT operation; 
		LOGGER.finest("PUT: Is invoked");
		operation = new OperationsDHT(OperationEnum.PUT_MAP, map, true);
		int nodes[] = tableManager.getNodes(map.getKey());
		Operacion datosOperacion = new Operacion(operation, nodes, nReplicas);
		byte[] bytes = serialize(datosOperacion); //Serializar datos de la operacion
		zkOperation op = new zkOperation(bytes);
		
		return operation.getValue();
	}

	private Integer putLocal(DHT_Map map) {
		DHTUserInterface  hashMap;
		hashMap = tableManager.getDHT(map.getKey());
		if (hashMap == null) {
			LOGGER.warning("Error: this sentence should not get here");
		}		
		return hashMap.put(map);
	}

	@Override
	public Integer get(String key) {
		java.util.List<String> DHTReplicas = new java.util.ArrayList<String>();
		OperationsDHT operation; 
		for (Iterator<String> iterator = DHTReplicas.iterator(); iterator.hasNext();) {
			String address = (String) iterator.next();
			LOGGER.finest("PUT: The operation is replicated");
			if (tableManager.isDHTLocalReplica(key, address)) {
				LOGGER.fine("PUT: Local replica");
				return getLocal(key);
			}
		}
		// Notify the operation to the cluster
		if (tableManager.isDHTLocal(key)) {
			LOGGER.finest("GET: The operation is local");
			return getLocal(key);
		} else {
			operation = mutex.sendOperation();
			LOGGER.fine("Returned value in get: " + operation.getValue());
			return operation.getValue();
		}
	}

	private Integer getLocal(String key) {
		DHTUserInterface  hashMap;
		hashMap = tableManager.getDHT(key);
		if (hashMap == null) {
			LOGGER.warning("Error: this sentence should not get here");
		}
		return hashMap.get(key);		
	}
	
	@Override
	public Integer remove(String key) {
		OperationsDHT operation; 
		LOGGER.finest("REMOVE: Is invoked");
		int value;
		// Create the array of nodes where map should be stored
		int nodes[] = tableManager.getNodes(key);
		for (int i = 1; i < nodes.length; i++) {
			if (tableManager.isDHTLocalReplica(nodes[i], key)) {
				LOGGER.fine("PUT: Local replica");
				value = removeLocal(key);
			} else {
				LOGGER.fine("REMOVE: Remote replica");
			}
		}
		if (tableManager.isDHTLocal(nodes[0])) {
			LOGGER.finest("PUT: The operation is local");
			return removeLocal(key);
		} else {
			operation = mutex.sendOperation();
			LOGGER.finest("Returned value in put: " + operation.getValue());
			return operation.getValue();
		}
	}

	private Integer removeLocal(String key) {
		DHTUserInterface  hashMap;
		hashMap = tableManager.getDHT(key);
		if (hashMap == null) {
			LOGGER.warning("Error: this sentence should not get here");
		}
		return hashMap.remove(key);		
	}
	
	@Override
	public boolean containsKey(String key) {
		Integer isContained = get(key);
		if (isContained == null) {
			return false;
		} else {
			return true;
		}
	}
	
	@Override
	public Set<String> keySet() {
		return null; //hashMap.keySet();
	}

	@Override
	public ArrayList<Integer> values() {
		return null;//hashMap.values();
	}

	@Override
	public String toString() {
		return tableManager.toString();
	}
}
