package es.upm.dit.dscc.DHT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TableManager {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private int       nReplica;
	private int       nServersMax;
	private String    localAddress;
	private HashMap<Integer, String> DHTServers = new HashMap<Integer, String>();
	private HashMap<Integer, DHTUserInterface> DHTTables = new HashMap<Integer, DHTUserInterface>();

	public TableManager(String localAddress,
			int     nServersMax, 
			int     nReplica) {

		this.localAddress = localAddress;
		this.nServersMax  = nServersMax;
		this.nReplica     = nReplica;
	}

	// TODO TRY TO MAKE THIS PRIVATE
	public Integer getPos (String key) {
		int hash =	key.hashCode();
		if (hash < 0) {
			LOGGER.finest("Hash value is negative!!!!!");
			hash = -hash;
		}
		int segment = Integer.MAX_VALUE / (nServersMax); // No negatives
		for(int i = 0; i < nServersMax; i++) {
			if (hash >= (i * segment) && (hash <  (i+1)*segment)){
				return i;
			}
		}
		LOGGER.warning("getPos: This sentence shound not run");
		return 1;
	}

	public Integer getPosicion (String address) {
		int posAddress = 0;
		for (int i = 0; i < DHTServers.size(); i++){
			if (localAddress.equals(DHTServers.get(i))) {
				posAddress = i;
				continue;
			}
		}
		return posAddress;
	}

	public int[] getNodes(String key) {
		int pos = getPos(key);
		int[] nodes = new int[nReplica];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = (pos + i) % nServersMax;
		}
		return nodes;
	}

	public void addDHT(DHTUserInterface dht, int pos) {
		DHTTables.put(pos, dht);
	}

	public DHTUserInterface getDHT(String key) {
		return DHTTables.get(getPos(key));
	}
	
	public HashMap<Integer, DHTUserInterface> getDHTTables() {
		return DHTTables;
	}

	HashMap<Integer, String> getDHTServers() {
		return DHTServers;
	}
	
	public String printDHTServers() {
		String aux = "DHTManager: Servers => [";
		for (int i = 0; i < nServersMax; i++) {
			if (DHTServers.get(i) != null) {
				aux = aux + DHTServers.get(i) + " ";
			} else {
				aux = aux + "null ";	
			}	
		}	
		aux = aux + "]";
		return aux;
	}

	public String getLocalAddress() {
		return localAddress;
	}

	public void setLocalAddress(String localAddress) {
		this.localAddress = localAddress;
	}

	@Override
	public String toString() {
		DHTUserInterface dht;
		String aux = "Size: " + DHTTables.size() + " Local server: " + getPosicion(localAddress) +"\n";
		aux = aux + printDHTServers() + "\n";
		for (int i = 0; i < nServersMax; i ++) {
			dht = DHTTables.get(i);
			if (dht == null) {
				aux = aux + "Table " + i + ": null" + "\n" ; 
			} else {
				aux = aux + "Table " + i + ": " + dht.toString() + "\n";
			}
		}
		return aux;
	}
}