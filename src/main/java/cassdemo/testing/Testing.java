package cassdemo.testing;

import cassdemo.StressTest;
import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.tables.*;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Testing {
    public final BackendSession session;

    public StressTest(BackendSession session) {
        this.session = session;
    }

    public void seed() throws BackendException {
        Byte S = 1, M = 2, L = 3;

        // lockers
        session.insertLocker("POZ123", S, M, L, L, M, S);

        // shipments
        session.insertShipment("Allegro", M);
        session.insertShipment("Nike", S);
        session.insertShipment("Adidas", S);
        session.insertShipment("Decathlon", L);
        session.insertShipment("Puma", S);
        session.insertShipment("Rebook", S);
        session.insertShipment("Action", L);

        List<Locker> lockers = session.selectAllLockers();
        List<Shipment> shipments = session.selectAllShipments();

        for (int i = 0; i < shipments.size(); i++) {
            session.insertShipmentIntoLocker(lockers.get(0).getLocker_id(), shipments.get(i).getShipment_id());
        }
    }
}
