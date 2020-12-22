package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.Set;

import org.jgroups.Address;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.JChannel;
import org.jgroups.Message;

public class DHTManager extends ReceiverAdapter  implements DHTUserInterface {

	private java.util.logging.Logger LOGGER = DHTMain.LOGGER;

	private int               nServersMax  = 3;
	private int               nReplica     = 2;
	private JChannel          channel;
	private SendMessagesDHT   sendMessages;
	private operationBlocking mutex;
	private Address           localAddress;
	private TableManager      tableManager;
	private ReceiveMessages   receiveMessages;
	private boolean           endConfigure = false;
	private DHTUserInterface  dht;
	private ViewManager       viewManager;

	public DHTManager (String cluster) {

		LOGGER.warning("Start of configuration " + cluster);

		try {
			channel = new JChannel().setReceiver(this);
			localAddress = channel.address();
			channel.connect(cluster);
			localAddress = channel.address();
		} catch (Exception e) {
			LOGGER.severe("Error to create the JGroups channel");
		}

		if (!endConfigure) {
			configure();
		}

		LOGGER.finest("End of configuration");
	}

	private void configure() {
		this.sendMessages    = new SendMessagesDHT(channel);
		this.mutex           = new operationBlocking();
		if (localAddress == null) {
			channel.address();
		}
		if (localAddress == null) {
			LOGGER.severe("localAddress is null!!!!");
		}
		this.tableManager    = new TableManager(localAddress, nServersMax, nReplica);
		this.dht             = new DHTJGroups(sendMessages, mutex, tableManager);
		this.viewManager     = new ViewManager(localAddress, nServersMax, nReplica,
									sendMessages, tableManager);
		this.receiveMessages = new ReceiveMessages(sendMessages, dht, mutex, viewManager);
		this.endConfigure    = true;
	}

	// View
	@Override
	public void viewAccepted(View newView) { 
		
		if (localAddress == null) {
			localAddress = newView.getMembers().get(newView.size() - 1);
		}

		if (!endConfigure) {
			configure();
		}

		try {

			LOGGER.warning("** view: " + newView);

			if (tableManager == null || newView == null) {
				LOGGER.severe("table or view is null");
			} else {
				viewManager.manageView(newView);					
			}

		} catch (Exception e) {
			LOGGER.severe("Unexpected exception in viewAccepted");
			System.err.println(e);
			e.printStackTrace();
		}
	}

	@Override
	public void receive(Message msg) {
		java.lang.Object dhtbdObj = null;

		try {
			dhtbdObj = (java.lang.Object) org.jgroups.util.Util.objectFromByteBuffer(msg.getBuffer());			
			LOGGER.finest("A message received");
		} catch (Exception e) {
			System.out.println(e.getStackTrace());
			System.out.println(e.toString());
			System.out.println("Exception objectFromByteBuffer");
		}
		OperationsDHT operation = (OperationsDHT) dhtbdObj;
		
		receiveMessages.handleReceiverMsg(msg.src(), operation);
	}
	
	public boolean isQuorum() {
		return viewManager.isQuorun();
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




