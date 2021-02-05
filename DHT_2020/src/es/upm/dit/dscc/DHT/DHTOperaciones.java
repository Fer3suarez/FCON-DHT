package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
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
//-----------------------------------------------------------------------------------------------------------------------------
//-----------------------------------------------------OPERACIONES----------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------------
	@Override
	public Integer put(DHT_Map map) {
	
		OperationsDHT operation; 
		LOGGER.fine("PUT: Is invoked");
		operation = new OperationsDHT(OperationEnum.PUT_MAP, map);
		int nodes[] = tableManager.getNodes(map.getKey());
		Operacion datosOperacion = new Operacion(operation, nodes, nReplicas);
		byte[] bytes = serialize(datosOperacion); //Serializar datos de la operacion
		zkMember.crearZnodeOperacion(bytes);
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
		OperationsDHT operation; 
		LOGGER.fine("GET: Is invoked");
		operation = new OperationsDHT(OperationEnum.GET_MAP, key);
		int nodes[] = tableManager.getNodes(key);
		Operacion datosOperacion = new Operacion(operation, nodes, nReplicas);
		byte[] bytes = serialize(datosOperacion); //Serializar datos de la operacion
		zkMember.crearZnodeOperacion(bytes);
		return operation.getValue();
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
		LOGGER.fine("GET: Is invoked");
		operation = new OperationsDHT(OperationEnum.REMOVE_MAP, key);
		int nodes[] = tableManager.getNodes(key);
		Operacion datosOperacion = new Operacion(operation, nodes, nReplicas);
		byte[] bytes = serialize(datosOperacion); //Serializar datos de la operacion
		zkMember.crearZnodeOperacion(bytes);
		return operation.getValue();
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
	
	@Override
	public Integer putMsg(DHT_Map map) {
		return putLocal(map);
	}

	@Override
	public Integer getMsg(String key) {
		// TODO Auto-generated method stub
		return getLocal(key);
	}

	@Override
	public Integer removeMsg(String key) {
		// TODO Auto-generated method stub
		return removeLocal(key);
	}
//-----------------------------------------------------------------------------------------------------------------------------
//-----------------------------------------------------SERIALIZACIONES---------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------------
	
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
}
