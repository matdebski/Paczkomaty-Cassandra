package cassdemo.tables;

import com.datastax.driver.mapping.annotations.Table;
import java.util.UUID;
import java.util.List;

@Table(name = "lockers")
public class Locker {
    private UUID locker_id;
    private String locker_name;
    private List<Integer> locker_boxes;

    public UUID getLocker_id() {
        return locker_id;
    }

    public void setLocker_id(UUID locker_id) {
        this.locker_id = locker_id;
    }

    public String getLocker_name() {
        return locker_name;
    }

    public void setLocker_name(String locker_name) {
        this.locker_name = locker_name;
    }

    public List<Integer> getLocker_boxes() {
        return locker_boxes;
    }

    public void setLocker_boxes(List<Integer> locker_boxes) {
        this.locker_boxes = locker_boxes;
    }

    @Override
    public String toString() {
        return "Lockers{" +
                "locker_id=" + locker_id +
                ", locker_name='" + locker_name + '\'' +
                ", locker_boxes=" + locker_boxes +
                '}';
    }
}
