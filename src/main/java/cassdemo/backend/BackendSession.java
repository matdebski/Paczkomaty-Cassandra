package cassdemo.backend;

import java.util.*;
import java.time.Instant;
import java.util.stream.Collectors;
import cassdemo.tables.*;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);
	public static MappingManager manager = null;
	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		logger.debug("Backend starting");
		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		cluster.getConfiguration().getCodecRegistry().register(InstantCodec.instance);
		try {
			session = cluster.connect(keyspace);
			manager = new MappingManager(session);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
		logger.debug("Backend successfully started");
	}

	/* Retrieve all records from each table */
	private static PreparedStatement SELECT_ALL_FROM_LOCKERS;
	private static PreparedStatement SELECT_ALL_FROM_SHIPMENTS;
	private static PreparedStatement SELECT_ALL_FROM_LOCKER_SHIPMENTS;
	private static PreparedStatement SELECT_ALL_FROM_SHIPMENT_LOCKERS;

	/* Retrieve all shipments stored in a specific locker or all lockers containing a specific shipment */
	private static PreparedStatement SELECT_ALL_SHIPMENTS_FROM_LOCKER_BY_ID;
	private static PreparedStatement SELECT_ALL_LOCKERS_FROM_SHIPMENT_BY_ID;

	private static PreparedStatement SELECT_ONE_FROM_LOCKERS;
	private static PreparedStatement SELECT_ONE_FROM_SHIPMENTS;
	private static PreparedStatement SELECT_ONE_FROM_LOCKER_SHIPMENTS;
	private static PreparedStatement SELECT_ONE_FROM_SHIPMENT_LOCKERS;

	private static PreparedStatement INSERT_INTO_LOCKERS;
	private static PreparedStatement INSERT_INTO_SHIPMENTS;
	private static PreparedStatement INSERT_SHIPMENT_INTO_LOCKER;

	private static PreparedStatement INSERT_SHIPMENT_LOCKER;
	private static PreparedStatement INSERT_LOCKER_SHIPMENT;

	private static PreparedStatement DELETE_ALL_FROM_LOCKERS;
	private static PreparedStatement DELETE_ALL_FROM_SHIPMENTS;
	private static PreparedStatement DELETE_ALL_FROM_LOCKER_SHIPMENTS;
	private static PreparedStatement DELETE_ALL_FROM_SHIPMENT_LOCKERS;

	/* Remove relationship between locker and shipment */
	private static PreparedStatement DELETE_SHIPMENT_FROM_LOCKER_BY_ID;

	private void prepareStatements() throws BackendException {
		logger.debug("Preparing statements / queries");
		try {
			SELECT_ALL_FROM_LOCKERS = session.prepare("SELECT * FROM lockers;");
			SELECT_ALL_FROM_SHIPMENTS = session.prepare("SELECT * FROM shipments;");
			SELECT_ALL_FROM_LOCKER_SHIPMENTS = session.prepare("SELECT * FROM locker_shipments;");
			SELECT_ALL_FROM_SHIPMENT_LOCKERS = session.prepare("SELECT * FROM shipment_lockers;");

			SELECT_ALL_SHIPMENTS_FROM_LOCKER_BY_ID = session.prepare("SELECT * FROM locker_shipments WHERE locker_id=?;");
			SELECT_ALL_LOCKERS_FROM_SHIPMENT_BY_ID = session.prepare("SELECT * FROM shipment_lockers WHERE shipment_id=?;");

			SELECT_ONE_FROM_LOCKERS = session.prepare("SELECT * FROM lockers WHERE locker_id=?;");
			SELECT_ONE_FROM_SHIPMENTS = session.prepare("SELECT * FROM shipments WHERE shipment_id=?;");
			SELECT_ONE_FROM_LOCKER_SHIPMENTS = session.prepare("SELECT * FROM locker_shipments WHERE locker_id=? AND shipment_id=?;");
			SELECT_ONE_FROM_SHIPMENT_LOCKERS = session.prepare("SELECT * FROM shipment_lockers WHERE shipment_id=? AND locker_id=?;");

			INSERT_INTO_LOCKERS = session.prepare(
					"INSERT INTO lockers (locker_id, locker_name, locker_boxes) VALUES (?, ?, ?);");
			INSERT_INTO_SHIPMENTS = session.prepare(
					"INSERT INTO shipments (shipment_id, shipment_name,box_size) VALUES (?, ?,?);");
			INSERT_SHIPMENT_INTO_LOCKER = session.prepare(
					"BEGIN BATCH " +
							"INSERT INTO locker_shipments (locker_id, shipment_id, locker_box_index, addedAt, status) VALUES (?, ?, ?, ?, ?);" +
							"INSERT INTO shipment_lockers (shipment_id, locker_id, locker_box_index, addedAt, status) VALUES (?, ?, ?, ?, ?);" +
							"APPLY BATCH;"
			);

			INSERT_LOCKER_SHIPMENT = session.prepare(
							"INSERT INTO locker_shipments (locker_id, shipment_id, locker_box_index, addedAt, status) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS;"
			);

			INSERT_SHIPMENT_LOCKER = session.prepare(
							"INSERT INTO shipment_lockers (shipment_id, locker_id, locker_box_index, addedAt, status) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS;"
			);

			DELETE_ALL_FROM_LOCKERS = session.prepare("TRUNCATE lockers;");
			DELETE_ALL_FROM_SHIPMENTS = session.prepare("TRUNCATE shipments;");
			DELETE_ALL_FROM_LOCKER_SHIPMENTS = session.prepare("TRUNCATE locker_shipments;");
			DELETE_ALL_FROM_SHIPMENT_LOCKERS = session.prepare("TRUNCATE shipment_lockers;");

			/* Initialize query for removing shipment-locker relationship atomically */
			DELETE_SHIPMENT_FROM_LOCKER_BY_ID = session.prepare(
					"BEGIN BATCH " +
							"DELETE FROM locker_shipments WHERE locker_id=? AND shipment_id=?; " +
							"DELETE FROM shipment_lockers WHERE shipment_id=? AND locker_id=?; " +
							"APPLY BATCH;"
			);

		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.debug("Statements and queries prepared");
	}

	/* CRUD operations */

	/* select */

	public List<Locker> selectAllLockers() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_LOCKERS);
		Mapper<Locker> mapper = manager.mapper(Locker.class);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch(Exception e) {
			throw new BackendException("Could not execute statement. " + e.getMessage() + ".", e);
		}

		return mapper.map(rs).all();
	}

	public List<Shipment> selectAllShipments() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_SHIPMENTS);
		Mapper<Shipment> mapper = manager.mapper(Shipment.class);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch(Exception e) {
			throw new BackendException("Could not execute statement. " + e.getMessage() + ".", e);
		}

		return mapper.map(rs).all();
	}

	public List<LockerShipment> selectAllShipmentsFromLockerById(UUID lockerId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_SHIPMENTS_FROM_LOCKER_BY_ID);
		bs.bind(lockerId);
		Mapper<LockerShipment> mapper = manager.mapper(LockerShipment.class);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch(Exception e) {
			throw new BackendException("Could not execute statement. " + e.getMessage() + ".", e);
		}

		return mapper.map(rs).all();
	}

	public Locker selectLocker(UUID lockerId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ONE_FROM_LOCKERS);
		bs.bind(lockerId);
		Mapper<Locker> mapper = manager.mapper(Locker.class);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch(Exception e) {
			throw new BackendException("Could not execute statement. " + e.getMessage() + ".", e);
		}

		return mapper.map(rs).one();
	}

	public Shipment selectShipment(UUID shipmentId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ONE_FROM_SHIPMENTS);
		bs.bind(shipmentId);
		Mapper<Shipment> mapper = manager.mapper(Shipment.class);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch(Exception e) {
			throw new BackendException("Could not execute statement. " + e.getMessage() + ".", e);
		}

		return mapper.map(rs).one();
	}

	public ShipmentLocker selectShipmentLocker(UUID shipmentId, UUID lockerId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ONE_FROM_SHIPMENT_LOCKERS);
		bs.bind(shipmentId, lockerId);
		Mapper<ShipmentLocker> mapper = manager.mapper(ShipmentLocker.class);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch(Exception e) {
			throw new BackendException("Could not execute statement. " + e.getMessage() + ".", e);
		}

		return mapper.map(rs).one();
	}

	/* insert */

	public void insertLocker(String lockerName, Byte... locker_boxes) throws BackendException {
		UUID newUUID = UUID.randomUUID();

		BoundStatement bs = new BoundStatement(INSERT_INTO_LOCKERS);
		bs.bind(newUUID, lockerName, Arrays.asList(locker_boxes));

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an insert. " + e.getMessage() + ".", e);
		}

		logger.info("Locker " + lockerName + " inserted with id: " + newUUID);
	}

	public void insertShipment(String shipmentName, Byte boxSize) throws BackendException {
		UUID newUUID = UUID.randomUUID();

		BoundStatement bs = new BoundStatement(INSERT_INTO_SHIPMENTS);
		bs.bind(newUUID, shipmentName,boxSize);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an insert. " + e.getMessage() + ".", e);
		}

		logger.info("Shipment " + shipmentName + " inserted with id: " + newUUID+"size: "+boxSize);
	}

	public void insertShipmentIntoLocker(UUID locker_id, UUID  shipment_id, Instant timestamp) throws BackendException {
		//locker_id, shipment_id, locker_box_index, addedAt, status

		Locker locker=selectLocker(locker_id);
		Shipment shipment=selectShipment(shipment_id);

		Byte shipmentSize = shipment.getBox_size();
		List<Byte> lockerBoxes = locker.getLocker_boxes();

		//Indexes of lockerBoxes with size>=shipmentsize
		List<Integer> availableIndices = new ArrayList<>();
		for (int i = 0; i < lockerBoxes.size(); i++) {
			if (lockerBoxes.get(i) >= shipmentSize) {
				availableIndices.add(i);
			}
		}

		// Sort by size and random when equal to minimize conflicts
		availableIndices.sort((a, b) -> {
			int sizeCompare = lockerBoxes.get(a).compareTo(lockerBoxes.get(b));
			if (sizeCompare == 0) {
				return new Random().nextInt(3) - 1;
			}
			return sizeCompare;
		});
		List<LockerShipment> lockerShipments= selectAllShipmentsFromLockerById(locker_id);

		//Based on lockerShipments check which lockers from availableIndices are free, not have status  "CONFIRMED"

		Set<Integer> occupiedIndices = lockerShipments.stream()
				.filter(lockershipment -> lockershipment.getStatus().equals("CONFIRMED"))
				.map(LockerShipment::getLocker_box_index)
				.collect(Collectors.toSet());

		availableIndices.removeIf(occupiedIndices::contains);
		BoundStatement bs;
		boolean confirmed=false;
		for (Integer index : availableIndices) {
			 bs= new BoundStatement(INSERT_SHIPMENT_LOCKER);
			 bs.bind(locker_id, shipment_id, index, timestamp, "WAITING");
			 try {
				session.execute(bs);
			 } catch (Exception e) {
				throw new BackendException("Could not perform insert operation. " + e.getMessage() + ".", e);
			 }

			bs= new BoundStatement(INSERT_LOCKER_SHIPMENT);
			bs.bind(locker_id, shipment_id, index, timestamp, "WAITING");
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform insert operation. " + e.getMessage() + ".", e);
			}

			confirmed=validateInsert(locker_id,shipment_id,index);

			 if(confirmed) {
				 bs= new BoundStatement(INSERT_SHIPMENT_LOCKER);
				 bs.bind(locker_id, shipment_id, index, timestamp, "CONFIRMED");
				 try {
					 session.execute(bs);
				 } catch (Exception e) {
					 throw new BackendException("Could not perform insert operation. " + e.getMessage() + ".", e);
				 }

				 bs= new BoundStatement(INSERT_LOCKER_SHIPMENT);
				 bs.bind(locker_id, shipment_id, index, timestamp, "CONFIRMED");
				 try {
					 session.execute(bs);
				 } catch (Exception e) {
					 throw new BackendException("Could not perform insert operation. " + e.getMessage() + ".", e);
				 }
				 break;
			 }
			 else{
				 bs= new BoundStatement(INSERT_SHIPMENT_LOCKER);
				 bs.bind(locker_id, shipment_id, index, timestamp, "REJECTED");
				 try {
					 session.execute(bs);
				 } catch (Exception e) {
					 throw new BackendException("Could not perform insert operation. " + e.getMessage() + ".", e);
				 }

				 bs= new BoundStatement(INSERT_LOCKER_SHIPMENT);
				 bs.bind(locker_id, shipment_id, index, timestamp, "REJECTED");
				 try {
					 session.execute(bs);
				 } catch (Exception e) {
					 throw new BackendException("Could not perform insert operation. " + e.getMessage() + ".", e);
				 }
			 }
		}
		System.out.println("insertResult: "+confirmed);
		//toDO if confirmed==false - there is no lockerbox available,insertion unsuccesfull

	}

	boolean validateInsert(UUID locker_id, UUID  shipment_id,int index) throws BackendException{
		List<LockerShipment> lockerShipments= selectAllShipmentsFromLockerById(locker_id);

		Instant timestamp=selectShipmentLocker(shipment_id,locker_id).getAddedAt();

		// Choose shipments assigned to same locker box index
		List<LockerShipment> filteredShipments = lockerShipments.stream()
				.filter(shipment -> shipment.getLocker_box_index() == index)
				.collect(Collectors.toList());

		// Check if any shipment has status Confirmed
		boolean isOccupied = filteredShipments.stream()
				.anyMatch(shipment -> shipment.getStatus().equals("CONFIRMED"));

		if (isOccupied) {
			return false;
		}

		// Check if any shipment has an earlier timestamp
		boolean isNotFirst = filteredShipments.stream()
				.anyMatch(shipment -> shipment.getAddedAt().isBefore(timestamp));


		if (isNotFirst) {
			return false;
		}

		return true;
	}

	public void insertShipmentIntoLocker(UUID locker_id, UUID  shipment_id) throws BackendException{
		insertShipmentIntoLocker(locker_id,shipment_id,Instant.now());
	}

	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_LOCKERS);
		BoundStatement bs1 = new BoundStatement(DELETE_ALL_FROM_SHIPMENTS);
		BoundStatement bs2 = new BoundStatement(DELETE_ALL_FROM_LOCKER_SHIPMENTS);
		BoundStatement bs3 = new BoundStatement(DELETE_ALL_FROM_SHIPMENT_LOCKERS);

		try {
			session.execute(bs);
			session.execute(bs1);
			session.execute(bs2);
			session.execute(bs3);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		logger.info("All data deleted");
	}

	public int checkLocker(UUID locker_id) throws BackendException {
		List<LockerShipment> lockerShipments = selectAllShipmentsFromLockerById(locker_id);

		Map<Integer, Long> indexCounts = lockerShipments.stream()
				.collect(Collectors.groupingBy(LockerShipment::getLocker_box_index, Collectors.counting()));

		Map<Integer, Long> duplicates = indexCounts.entrySet().stream()
				.filter(entry -> entry.getValue() > 1)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		if (duplicates.isEmpty()) {
			logger.info("Success: don't found duplicates in locker " + locker_id);
		} else {
			logger.info("Failure: found " + duplicates.size() + " duplicates in locker " + locker_id);
		}

		return duplicates.size();
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
