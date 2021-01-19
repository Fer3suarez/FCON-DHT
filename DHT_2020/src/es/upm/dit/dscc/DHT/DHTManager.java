package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.Set;


public class DHTManager implements DHTUserInterface {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;

	private int               nServersMax  = 3;
	private int               nReplica     = 2;
	private operationBlocking mutex;
	private String           localAddress;
	private TableManager      tableManager;
	private boolean           endConfigure = false;
	private DHTUserInterface  dht;

	public DHTManager () {

		LOGGER.warning("Start of configuration ");


		if (!endConfigure) {
			configure();
		}

		LOGGER.finest("End of configuration");
	}

	private void configure() {
		this.mutex           = new operationBlocking();
		this.tableManager    = new TableManager(localAddress, nServersMax, nReplica);
		this.dht             = new DHTOperaciones(mutex, tableManager);
		this.endConfigure    = true;
	}

	public boolean isQuorum() {
		return false; //viewManager.isQuorun();
	}
	
	public Integer put(DHT_Map map) {
		return dht.put(map);
	}
	
	public Integer putMsg(DHT_Map map) {
		return null;
	}
	
	public Integer get(String key) {
		return dht.get(key);
	}
	
	//Se añade
	public Integer getMsg(String key) {
		return null;
	}

	public Integer remove(String key) {
		return dht.remove(key);
	}
	
	public Integer removeMsg(String key) {
		return null;
	}
	
	public boolean containsKey(String key) {
		return dht.containsKey(key);
	}

	public Set<String> keySet() {
		return dht.keySet();
	}

	public ArrayList<Integer> values() {
		return dht.values();
	}
	
	public String getServers() {
		return tableManager.printDHTServers();
	}
	
	@Override
	public String toString() {
		return dht.toString();
	}



}




