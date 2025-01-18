package cassdemo.backend;

import java.util.*;
import java.time.Instant;
import java.util.stream.Collectors;
import cassdemo.tables.*;
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
