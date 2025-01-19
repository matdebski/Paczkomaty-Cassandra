package cassdemo.testing;

import cassdemo.backend.BackendException;
import cassdemo.testing.*;

public class Stress implements Runnable{

    private final Testing testing;

    public Stress(Testing testing) {
        this.testing = testing;
    }

    @Override public void run() {
        try {
            testing.addRandomShipmentsToRandomLockers();
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }
}