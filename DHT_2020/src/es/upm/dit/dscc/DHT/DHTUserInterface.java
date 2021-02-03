package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.Set;

public interface DHTUserInterface {

	Integer put(DHT_Map map);
	
	Integer get(String key);

	Integer remove(String key);

	boolean containsKey(String key);

	Set<String> keySet();
	
	ArrayList<Integer> values();

	Integer putMsg(DHT_Map map);
	Integer getMsg(String key);
	Integer removeMsg(String key);

}