package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.io.Serializable;

public class DHTHashMap implements DHTUserInterface, Serializable{
	
	private static final long serialVersionUID = 1L;
	private HashMap <String, Integer> hashMap = new HashMap<String, Integer>();
		
	@Override
	public Integer put(DHT_Map map) {
		if (map == null) {
			return null;
		}
		hashMap.put(map.getKey(), map.getValue());
		return map.getValue();
	}
	
	@Override
	public Integer get(String key) {
		return hashMap.get(key);
	}
	
	@Override
	public Integer remove(String key) {
		return hashMap.remove(key);
	}
	
	@Override
	public boolean containsKey(String key) {
		return hashMap.containsKey(key);
	}
	
	@Override
	public Set<String> keySet() {
		return hashMap.keySet();
	}
	
	@Override
	public ArrayList<Integer> values() {
		
		Collection<Integer> values;
		ArrayList<Integer> list = new ArrayList<Integer>();
		values = hashMap.values();
		for (Iterator<Integer> iterator = values.iterator(); iterator.hasNext();) {
			Integer integer = iterator.next();
			list.add(integer);
		}
		return list;
	}

	public HashMap <String, Integer> getDHT() {
		return hashMap;
	}
	
	@Override
	public String toString() {
		String aux = "";
		ArrayList<Integer> list = new ArrayList<Integer>();
		list = 	this.values();
		for (Iterator<Integer> iterator = list.iterator(); iterator.hasNext();) {
			Integer values = iterator.next();
			aux = aux + " " + values;
		}
		return aux;
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
