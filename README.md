# Parcel Locker System with NoSQL Cassandra

## 📘 Project Overview

This system handles parcel deliveries to automated parcel lockers. Each locker contains a predefined number of compartments of various sizes. The system’s main responsibility is to assign parcels to available compartments while ensuring that **no two parcels are assigned to the same compartment**.

As part of an experiment, we use the **Apache Cassandra** NoSQL database, which runs in a distributed cluster. Due to its architecture, **data consistency issues may arise**, especially in cases where two clients simultaneously connect to different cluster nodes and attempt to assign parcels to the same locker. This can lead to anomalies, such as assigning two parcels to a single compartment.

## 🗂️ Database Schema

```sql
CREATE TABLE IF NOT EXISTS lockers (
    locker_id uuid,
    locker_name text,
    locker_boxes list<tinyint>, -- list of box sizes: 1 = Small, 2 = Medium, 3 = Large
    PRIMARY KEY (locker_id)
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id uuid,
    shipment_name text,
    box_size tinyint, -- 1 = Small, 2 = Medium, 3 = Large
    PRIMARY KEY (shipment_id)
);

CREATE TABLE IF NOT EXISTS locker_shipments (
    locker_id uuid,
    shipment_id uuid,
    locker_box_index int, -- index of the box in locker_boxes list
    addedAt timestamp,
    status text, -- waiting / confirmed / rejected
    PRIMARY KEY (locker_id, shipment_id)
);

CREATE TABLE IF NOT EXISTS shipment_lockers (
    shipment_id uuid,
    locker_id uuid,
    locker_box_index int,
    addedAt timestamp,
    status text,
    PRIMARY KEY (shipment_id, locker_id)
);
```
## 🚚 Parcel Assignment Process
To reduce the risk of consistency anomalies in Cassandra, the parcel assignment process follows a two-phase approach:

### Step 1: Select Available Boxes
The system queries for free compartments in a locker that are equal to or larger than the size of the parcel.

A free compartment is one that has no record with status **CONFIRMED** in the locker_shipments table.

### Step 2: Attempt Assignment to Available Boxes
For each available compartment (in random order), the following steps are executed:

A temporary entry is created in locker_shipments and shipment_lockers with status **WAITING** and the current timestamp.

The system performs the following checks:

Is there existing confirmed assignment for that box?

Is there a waiting assignment with earlier timestamp?

If neither condition is met, the status is updated to **CONFIRMED**, successfully assigning the parcel.

Otherwise, the status is set to **REJECTED**, and the process continues with the next available compartment.

## Example test results:
![image](https://github.com/user-attachments/assets/30315a8d-6b19-4da9-afcf-64b9eb5e3f93)
![image](https://github.com/user-attachments/assets/eddb73f3-92df-45c4-a786-80598fcba01f)
As you can see, there are two records with the status "CONFIRMED" assigned to the same compartment (locker_box_index). Such anomalies can only be observed in a multi-node environment.

