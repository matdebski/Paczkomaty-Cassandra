package cassdemo.tables;

import com.datastax.driver.mapping.annotations.Table;
import java.util.UUID;
import java.time.Instant;

@Table(name = "shipment_lockers")
public class ShipmentLocker {
    private UUID shipment_id;
    private UUID locker_id;
    private int locker_box_index;
    private Instant addedAt;
    private String status;

    public UUID getShipment_id() {
        return shipment_id;
    }

    public void setShipment_id(UUID shipment_id) {
        this.shipment_id = shipment_id;
    }

    public UUID getLocker_id() {
        return locker_id;
    }

    public void setLocker_id(UUID locker_id) {
        this.locker_id = locker_id;
    }

    public int getLocker_box_index() {
        return locker_box_index;
    }

    public void setLocker_box_index(int locker_box_index) {
        this.locker_box_index = locker_box_index;
    }

    public Instant getAddedAt() {
        return addedAt;
    }

    public void setAddedAt(Instant addedAt) {
        this.addedAt = addedAt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "ShipmentLockers{" +
                "shipment_id=" + shipment_id +
                ", locker_id=" + locker_id +
                ", locker_box_index=" + locker_box_index +
                ", addedAt=" + addedAt +
                ", status='" + status + '\'' +
                '}';
    }
}