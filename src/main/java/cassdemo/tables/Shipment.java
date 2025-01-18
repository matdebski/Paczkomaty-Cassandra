package cassdemo.tables;

import com.datastax.driver.mapping.annotations.Table;
import java.util.UUID;
import java.util.List;

@Table(name = "shipments")
public class Shipment {
    private UUID shipment_id;
    private String shipment_name;
    private Byte box_size;

    public UUID getShipment_id() {
        return shipment_id;
    }

    public void setShipment_id(UUID shipment_id) {
        this.shipment_id = shipment_id;
    }

    public String getShipment_name() {
        return shipment_name;
    }

    public void setShipment_name(String shipment_name) {
        this.shipment_name = shipment_name;
    }

    public Byte getBox_size() {
        return box_size;
    }

    public void setBox_size(Byte box_size) {
        this.box_size = box_size;
    }

    @Override
    public String toString() {
        return "Shipments{" +
                "shipment_id=" + shipment_id +
                ", shipment_name='" + shipment_name + '\'' +
                '}';
    }
}
