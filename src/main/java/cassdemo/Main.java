package cassdemo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.tables.Locker;
import cassdemo.tables.Shipment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	public static void main(String[] args) throws IOException, BackendException {
		logger.debug("Main started");
		String contactPoint = null;
		String keyspace = null;

		Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
			
		BackendSession session = new BackendSession(contactPoint, keyspace);

		Scanner in = new Scanner(System.in);
		do {
			System.out.println(
					"Menu\nx - EXIT\ns - Add shipment\nl - Add locker\nss - Show shipments\nsl - Show lockers");
			String input = in.nextLine();
			switch (input) {
				case "x" -> {
					System.out.println("EXITING");
					session.deleteAll();
					System.exit(0);
				}
				case "s" -> {
					System.out.println("Type user name: ");
					String name = in.nextLine();
					System.out.println("Type box-size (1-3): ");
					byte boxsize = Byte.parseByte(in.nextLine());
					session.insertShipment(name, boxsize);
				}
				case "l" -> {
					System.out.println("Type locker name: ");
					String name = in.nextLine();
					System.out.println("Type locker boxes list (e.g., 1 2 3): ");

					String[] inputBoxes = in.nextLine().split(" ");
					Byte[] boxes_list = Arrays.stream(inputBoxes).map(Byte::parseByte).toArray(Byte[]::new);
					session.insertLocker(name, boxes_list);
				}
				case "ss" -> {
					System.out.println("Shipments:");
					try {
						List<Shipment> shipments = session.selectAllShipments();
						if (shipments.isEmpty()) {
							System.out.println("No shipments found.");
						} else {
							for (Shipment shipment : shipments) {
								System.out.println(shipment.toString());
							}
						}
					} catch (BackendException e) {
						System.err.println("Error fetching shipments: " + e.getMessage());
					}
				}
				case "sl" -> {
					System.out.println("Lockers:");
					try {
						List<Locker> lockers = session.selectAllLockers();
						if (lockers.isEmpty()) {
							System.out.println("No lockers found.");
						} else {
							for (Locker locker : lockers) {
								System.out.println(locker.toString());
							}
						}
					} catch (BackendException e) {
						System.err.println("Error fetching lockers: " + e.getMessage());
					}
				}
				default -> System.out.println("Invalid option. Please try again.");
			}
		} while (true);

	}



//		session.upsertUser("PP", "Adam", 609, "A St");
//		session.upsertUser("PP", "Ola", 509, null);
//		session.upsertUser("UAM", "Ewa", 720, "B St");
//		session.upsertUser("PP", "Kasia", 713, "C St");
//
//		String output = session.selectAll();
//		System.out.println("Users: \n" + output);
//
//		session.deleteAll();
	}
