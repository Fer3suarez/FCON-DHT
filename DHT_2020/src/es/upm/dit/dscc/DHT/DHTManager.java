package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.Set;


public class DHTManager implements DHTUserInterface {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;

	private int               nServersMax  = 3;
	private int               nReplica     = 2;
	private operationBlocking mutex;
	private String            localAddress;
	private TableManager      tableManager;
	private boolean           endConfigure = false;
	private DHTUserInterface  dht;
	private zkMember          zkMember;

	public DHTManager () {
		LOGGER.warning("Start of configuration ");
		if (!endConfigure) {
			configure();
		}
		this.localAddress = zkMember.getLocalAddress();
		LOGGER.finest("Añadido myId a localAddress: " + this.localAddress);
		this.tableManager.setLocalAddress(this.localAddress);
		LOGGER.finest("End of configuration");
	}

	private void configure() {
		this.mutex           = new operationBlocking();
		this.tableManager    = new TableManager(localAddress, nServersMax, nReplica);
		this.dht             = new DHTOperaciones(mutex, tableManager, nReplica);
		this.zkMember        = new zkMember(nServersMax, nReplica, mutex, tableManager, dht);
		this.endConfigure    = true;
	}

	public boolean isQuorum() {
		return zkMember.isQuorun();
	}
	
	@Override
	public Integer put(DHT_Map map) {
		return dht.put(map);
	}
	
	@Override
	public Integer get(String key) {
		return dht.get(key);
	}

	@Override
	public Integer remove(String key) {
		return dht.remove(key);
	}
	
	@Override
	public boolean containsKey(String key) {
		return dht.containsKey(key);
	}

	@Override
	public Set<String> keySet() {
		return dht.keySet();
	}

	@Override
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

	@Override
	public Integer putMsg(DHT_Map map) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getMsg(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer removeMsg(String key) {
		// TODO Auto-generated method stub
		return null;
	}
}