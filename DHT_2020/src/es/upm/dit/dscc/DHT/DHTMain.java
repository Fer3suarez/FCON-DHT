package es.upm.dit.dscc.DHT;

import java.util.Scanner;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DHTMain {

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format",
				"[%1$tF %1$tT][%4$-7s] [%5$s] [%2$-7s] %n");
	}

	static final Logger LOGGER = Logger.getLogger(DHTMain.class.getName());

	public DHTMain() {
		configureLogger();
	}

	public void configureLogger() {
		ConsoleHandler handler;
		handler = new ConsoleHandler(); 
		handler.setLevel(Level.FINE); 
		LOGGER.addHandler(handler); 
		LOGGER.setLevel(Level.FINE);
	}

	public void initMembers(DHTUserInterface dht) {
			dht.put(new DHT_Map("Angel", 1));
			dht.put(new DHT_Map("Bernardo", 2));
			dht.put(new DHT_Map("Carlos", 3));
			dht.put(new DHT_Map("Daniel", 4));
			dht.put(new DHT_Map("Eugenio", 5));
			dht.put(new DHT_Map("Zamorano", 6));
	}

	public DHT_Map putMap(Scanner sc) {
		String  key     = null;
		Integer value   = 0;
		
		System. out .print(">>> Enter name (String) = ");
		key = sc.next();
		System. out .print(">>> Enter account number (int) = ");
		if (sc.hasNextInt()) {
			value = sc.nextInt();
		} else {
			System.out.println("The provised text provided is not an integer");
			sc.next();
			return null;
		}
		return new DHT_Map(key, value);
	}
//-----------------------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------MAIN------------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------------------	

	public static void main(String[] args) {
		boolean correct = false;
		int     menuKey = 0;
		boolean exit    = false;
		Scanner sc      = new Scanner(System.in);
		String  key     = null;
		Integer value   = 0;
		DHTManager        dht        = new DHTManager();
		DHTMain           mainDHT    = new DHTMain();

		while (!exit) {
			try {
				correct = false;
				menuKey = 0;
				while (!correct) {
					System. out .println(">>> Enter option: 1) Put. 2) Get. 3) Remove. 4) ContainKey  5) Values 6) Init 0) Exit");				
					if (sc.hasNextInt()) {
						menuKey = sc.nextInt();
						correct = true;
					} else {
						sc.next();
						System.out.println("The provised text provided is not an integer");
					}
				}
				if (!dht.isQuorum()) {
					System.out.println("No hay quorum. No es posible ejecutar su elección");
					continue;
				}
				switch (menuKey) {
				case 1: // Put
					dht.put(mainDHT.putMap(sc));
					break;
				case 2: // Get
					System. out .print(">>> Enter key (String) = ");
					key    = sc.next();
					value  = dht.get(key);
					if (value != null) {
						System.out.println(value);							
					} else {
						System.out.println("The key: " + key + " does not exist");
					}
					break;
				case 3: // Remove
					System. out .print(">>> Enter key (String) = ");
					key    = sc.next();
					value  = dht.remove(key);
					if (value != null) {
						System.out.println(value);							
					} else {
						System.out.println("The key: " + key + " does not exist");
					}					
					break;
				case 4: // ContainKey
					System. out .print(">>> Enter key (String) = ");
					key    = sc.next();
					if (dht.containsKey(key)) {
						System.out.println("This key is contained");						
					} else {
						System.out.println("The option is not contained");						
					}
					break;
				case 5:
					System.out.println("List of values in the DHT:");
					System.out.println(dht.toString());
					break;
				case 7:
					mainDHT.initMembers(dht);
					break;
				case 0:
					exit = true;	
				default:
					System.out.println("The option is not available");
					break;
				}
			} catch (Exception e) {
				System.out.println("Exception at Main. Error read data");
				System.err.println(e);
				e.printStackTrace();
			}
		}
		sc.close();
	}
}