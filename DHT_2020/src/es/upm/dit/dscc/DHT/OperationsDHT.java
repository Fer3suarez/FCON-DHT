package es.upm.dit.dscc.DHT;

import java.io.Serializable;
//import java.util.Set;
import java.util.HashMap;

public class OperationsDHT implements Serializable {

	private static final long serialVersionUID = 1L;
	private OperationEnum operation;
	private Integer     value         = null;       
	private String      key           = null;
	private DHT_Map     map           = null;
	private boolean     status        = false;
	private DHTUserInterface dht      = null;
	private HashMap<Integer, String> DHTServers;

	// PUT_MAP
	public OperationsDHT (OperationEnum operation,
			DHT_Map map) {
		this.operation = operation;
		this.map       = map;
	}

	// GET_MAP REMOVE_MAP CONTAINS_KEY_MAP
	public OperationsDHT (OperationEnum operation,
			String key) {
		this.operation = operation;
		this.key       = key;
	}
	
	public OperationEnum getOperation() {
		return operation;
	}

	public void setOperation(OperationEnum operation) {
		this.operation = operation;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public DHT_Map getMap() {
		return map;
	}

	public void setMap(DHT_Map map) {
		this.map = map;
	}

	public boolean getStatus() {
		return status;
	}

	public void setMap(boolean status) {
		this.status = status;
	}

	public DHTUserInterface getDHT() {
		return this.dht;
	}

	public HashMap<Integer, String> getDHTServers() {
		return this.DHTServers;
	}
}

