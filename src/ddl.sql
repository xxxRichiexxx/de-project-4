-- STAGE --

CREATE TABLE stg.restaurants (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value VARCHAR NOT NULL,
    update_ts timestamp NOT NULL unique
);

CREATE TABLE stg.couriers (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value VARCHAR NOT NULL,
    update_ts timestamp NOT NULL unique
);

CREATE TABLE stg.deliveries (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL unique
);

-- CDM --

CREATE TABLE cdm.dm_courier_ledger (
    id INT GENERATED ALWAYS AS IDENTITY,
    courier_id INT NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year INT NOT NULL,
    settlement_month INT NOT NULL,
    orders_count INT NOT NULL,
    orders_total_sum real NOT NULL,
    rate_avg NUMERIC(3,2) NOT NULL,
    order_processing_fee real NOT NULL,
    courier_order_sum real NOT NULL,
    courier_tips_sum real NOT NULL,
    courier_reward_sum real NOT null,
    
  	constraint dm_courier_ledger_unique UNIQUE(courier_id, settlement_year, settlement_month)
);

-- DDS --

CREATE TABLE dds.couriers(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL
);

CREATE TABLE dds.calendar(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    "date" date NOT NULL,
    "timestamp" TIMESTAMP NOT NULL UNIQUE

);

CREATE TABLE dds.orders(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    calendar_id INT NOT NULL REFERENCES dds.calendar(id),
    "sum" real NOT NULL
);

CREATE TABLE dds.deliveries(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    order_id INT NOT NULL REFERENCES dds.orders(id),
    address TEXT NOT NULL,
    courier_id INT NOT NULL REFERENCES dds.couriers(id),
    rate INT,
    calendar_id INT NOT NULL REFERENCES dds.calendar(id),
    tip_sum real NOT NULL
);
