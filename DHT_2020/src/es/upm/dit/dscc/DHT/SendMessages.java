package es.upm.dit.dscc.DHT;

import java.util.ArrayList;

import org.jgroups.*;

public interface SendMessages {

	public void sendPut(Address address, DHT_Map map, boolean isReplica);

	public void sendGet(Address address, String key, boolean isReplica);

	public void sendRemove(Address address, String key, boolean isReplica);

	public void sendContainsKey(Address address, String key, boolean isReplica);

	public void sendKeySet (Address address);

	public void sendValues (Address address);
	
	public void sendList (Address address, ArrayList<String> list);

	public void returnValue(Address address, Integer value);

	public void returnStatus(Address address, boolean status);
}

