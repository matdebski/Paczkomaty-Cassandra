CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE IF NOT EXISTS lockers
(
    locker_id uuid,
    locker_name text,
    locker_boxes list<tinyint>, // lista skrytek w paczkomacie, 1 = Small, 2 = Medium, 3 = Large
    PRIMARY KEY (locker_id)
);

CREATE TABLE IF NOT EXISTS shipments
(
    shipment_id uuid,
    shipment_name text,
    box_size tinyint,
    PRIMARY KEY (shipment_id)
);

CREATE TABLE IF NOT EXISTS locker_shipments
(
    locker_id uuid,
    shipment_id uuid,
    locker_box_index int, // index na liście skrytek w paczkomacie
    addedAt timestamp,
    status text,
    PRIMARY KEY (locker_id, shipment_id)
);

CREATE TABLE IF NOT EXISTS shipment_lockers
(
    shipment_id uuid,
    locker_id uuid,
    locker_box_index int,
    addedAt timestamp,
    status text,
    PRIMARY KEY (shipment_id, locker_id)
);
