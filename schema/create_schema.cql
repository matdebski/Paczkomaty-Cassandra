CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE IF NOT EXISTS lockers
(
    locker_id uuid,
    locker_adres text,
    locker_boxes list<char>, //lista skrytek w paczkomacie
    PRIMARY KEY (locker_id)
    );

CREATE TABLE IF NOT EXISTS shipments
(
    shipment_id uuid,
    shipment_name text,
    PRIMARY KEY (shipment_id)
    );

CREATE TABLE IF NOT EXISTS locker_shipments
(
    locker_id uuid,
    shipment_id uuid,
    lockerShipmentIndex int, //index na liście skrytek w paczkomacie
    addedAt timestamp,
    status text, //do analizy o co z tym chodzi
    PRIMARY KEY (locker_id, shipment_id)
    );

CREATE TABLE IF NOT EXISTS shipment_locker
(
    shipment_id uuid,
    locker_id uuid,
    lockerShipmentIndex int,
    addedAt timestamp,
    status text,
    PRIMARY KEY (shipment_id, locker_id)
    );



CREATE TABLE Users (
  companyName varchar,
  name varchar,
  phone int,
  street varchar,
  pets list<varchar>,
  PRIMARY KEY (companyName, name)
);

CREATE INDEX streetIndex ON Users (street);

CREATE TABLE MeteoData (
  stationName varchar, 
  measurement timestamp, 
  temperature double,
  pressure double,
  PRIMARY KEY (stationName, measurement)
);

CREATE TABLE MeteoData2 (
  stationName varchar, 
  mDay varchar,
  measurement timestamp, 
  temperature double,
  pressure double,
  PRIMARY KEY ((stationName, mDay), measurement)
);

CREATE TABLE PageViewCounts (
  url varchar,
  views counter,
  PRIMARY KEY(url)
);