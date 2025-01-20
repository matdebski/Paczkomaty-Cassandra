package cassdemo.testing;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.tables.*;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Testing {
    private final int STRESS_TEST_THREADS = 5;

    private static final Logger logger = LoggerFactory.getLogger(Testing.class);
    public final BackendSession session;
    public static AtomicInteger duplicatesFound  = new AtomicInteger(0);

    public Testing(BackendSession session) {
        this.session = session;
    }

    public void seed() throws BackendException {
        Byte S = 1, M = 2, L = 3;

        // lockers
        session.insertLocker("POZ123", S, M, L, L, M, S);
        session.insertLocker("POZ421", L, M, L);
        session.insertLocker("POZ875", L, S, L, L, M, M, S);

        // 27 shipments
        session.insertShipment("Allegro", M);
        session.insertShipment("Nike", S);
        session.insertShipment("Adidas", S);
        session.insertShipment("Decathlon", L);
        session.insertShipment("Puma", S);
        session.insertShipment("Rebook", S);
        session.insertShipment("Action", L);
        session.insertShipment("Amazon", L);
        session.insertShipment("eBay", M);
        session.insertShipment("Walmart", S);
        session.insertShipment("Target", M);
        session.insertShipment("Costco", L);
        session.insertShipment("Zara", S);
        session.insertShipment("H&M", S);
        session.insertShipment("Uniqlo", M);
        session.insertShipment("Apple", L);
        session.insertShipment("Samsung", M);
        session.insertShipment("Microsoft", S);
        session.insertShipment("Google", M);
        session.insertShipment("Facebook", S);
        session.insertShipment("Tesla", L);
        session.insertShipment("BMW", L);
        session.insertShipment("Ford", M);
        session.insertShipment("Toyota", S);
        session.insertShipment("IKEA", L);
        session.insertShipment("Philips", M);
        session.insertShipment("Sony", S);

        List<Locker> lockers = session.selectAllLockers();
        List<Shipment> shipments = session.selectAllShipments();

//        for (int i = 0; i < 7; i++) {
//            session.insertShipmentIntoLocker(lockers.get(0).getLocker_id(), shipments.get(i).getShipment_id());
//        }
//
//        for (int i = 7; i < 17; i++) {
//            session.insertShipmentIntoLocker(lockers.get(1).getLocker_id(), shipments.get(i).getShipment_id());
//        }
//
//        for (int i = 17; i < 27; i++) {
//            session.insertShipmentIntoLocker(lockers.get(2).getLocker_id(), shipments.get(i).getShipment_id());
//        }
    }

    public void generateRandomShipmentsAndRandomLockers() throws BackendException {
        Byte S = 1, M = 2, L = 3;

        Random random = new Random();

        for (int i = 1; i <= 500; i++) {
            String lockerName = "LOCK" + i;
            int sizeCount = random.nextInt(5) + 1;
            Byte[] boxSizes = new Byte[sizeCount];
            for (int j = 0; j < sizeCount; j++) {
                int sizeRandom = random.nextInt(3);
                boxSizes[j] = sizeRandom == 0 ? S : sizeRandom == 1 ? M : L;
            }

            session.insertLocker(lockerName, boxSizes);
        }

        for (int i = 1; i <= 50; i++) {
            String shipmentName = "SHIP" + i;
            int sizeRandom = random.nextInt(3);
            Byte shipmentSize = sizeRandom == 0 ? S : sizeRandom == 1 ? M : L;

            session.insertShipment(shipmentName, shipmentSize);
        }
    }

    public void addRandomShipmentsToRandomLockers() throws BackendException {
        Random random = new Random();
        List<Locker> lockers = session.selectAllLockers();
        List<Shipment> shipments = session.selectAllShipments();

        for (int i = 0; i < lockers.size() * 2; i++) {
            session.insertShipmentIntoLocker(
                    lockers.get(random.nextInt(lockers.size())).getLocker_id(),
                    shipments.get(random.nextInt(shipments.size())).getShipment_id()
            );
        }
    }

    public void checkAllLockers() throws BackendException {
        List<Locker> lockers = session.selectAllLockers();

        for (Locker locker : lockers) {
            duplicatesFound.addAndGet(session.checkLocker(locker.getLocker_id()));
        }
    }

    public void stressTest() throws BackendException {
        duplicatesFound.set(0);

        System.out.print("START");

        this.session.deleteAll();
        this.generateRandomShipmentsAndRandomLockers();

        ExecutorService executorService = Executors.newFixedThreadPool(STRESS_TEST_THREADS);
        executorService = Executors.newFixedThreadPool(STRESS_TEST_THREADS);
        for (int i = 0; i < STRESS_TEST_THREADS; i++) {
            executorService.execute(new Stress(this));
        }
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        this.checkAllLockers();

        System.out.println("Stress test end!");
        System.out.println("Found " + duplicatesFound.get() + " duplicates");
    }
}
