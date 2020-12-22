package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.HashMap;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;

import java.util.logging.ConsoleHandler;
import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


public class SendMessagesDHT implements SendMessages{

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;
	private JChannel channel;

	public SendMessagesDHT(JChannel channel)  {
		this.channel = channel;
	}

	public void putChannel(JChannel channel) {
		this.channel = channel;
	}
	
	private void sendMessage(Address address, OperationsDHT operation) {

		try {
			if (address == null) LOGGER.warning("Address is null");
			if (operation == null) LOGGER.warning("Operation is null");
			
			byte[] bufferByte = org.jgroups.util.Util.objectToByteBuffer(operation);
			
			if (bufferByte == null) LOGGER.warning("Buffer is null");
			if (channel == null ) LOGGER.warning("Channel is null");
			Message msg = new Message(address, bufferByte);
			//if (msg == null ) LOGGER.warning("Message is null");
			channel.send(msg);
			LOGGER.fine("A message has been sent " + operation.getOperation() + " Address: " + address);
		} catch (Exception e) {
			System.err.println(e);
			System.out.println("Error when sending message");
			e.printStackTrace();
		}
	}

	//@Override
	public void sendPut(Address address, DHT_Map map, boolean isReplica) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.PUT_MAP, map, isReplica);
		sendMessage(address, operation);
	}

	//@Override
	public void sendGet(Address address, String key, boolean isReplica) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.GET_MAP, key, isReplica);
		sendMessage(address, operation);
	}

	public void sendRemove(Address address, String key, boolean isReplica) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.REMOVE_MAP, key, isReplica);
		sendMessage(address, operation);		
	}

	public void sendContainsKey(Address address, String key, boolean isReplica) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.CONTAINS_KEY_MAP, key, isReplica);
		sendMessage(address, operation);

	}

	public void sendKeySet (Address address) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.KEY_SET_HM);
		sendMessage(address, operation);
	}

	public void sendValues (Address address) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.VALUES_HM);
		sendMessage(address, operation);
	}

	public void sendList (Address address, ArrayList<String> list) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.LIST_SERVERS);
		sendMessage(address, operation);
	}
	
	public void returnValue(Address address, Integer value) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.RETURN_VALUE,
											value);
		sendMessage(address, operation);
	}

	public void returnStatus(Address address, boolean status) {
		OperationsDHT operation = new OperationsDHT(OperationEnum.RETURN_VALUE,
											status);
		sendMessage(address, operation);
	}
	
	public void sendServers(Address address, HashMap<Integer, Address> DHTServers ) {
		LOGGER.fine("sendServers. Address: " + address);
		System.out.println("sendServers. Address: " + address);
		OperationsDHT operation = new OperationsDHT(OperationEnum.DHT_REPLICA, DHTServers);
		sendMessage(address, operation);
		sendMessage(address, operation);
	}
	
	public void sendReplicaData(Address address, DHTUserInterface dht, 
			int posReplica, int posServer) {
		LOGGER.fine("Send DHT Replica. Address: " + address);
		OperationsDHT operation = new OperationsDHT(OperationEnum.DATA_REPLICA,
				dht, posReplica, posServer);
		sendMessage(address, operation);
		sendMessage(address, operation);
	}
	
	public void sendInit(Address address) {
		LOGGER.fine("Send DHT Replica. Address: " + address);
		OperationsDHT operation = new OperationsDHT(OperationEnum.INIT);
		sendMessage(address, operation);		
	}
}



