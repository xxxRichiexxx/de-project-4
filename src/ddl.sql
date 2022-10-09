-- STAGE --

CREATE TABLE IF NOT EXISTS stg.restaurants (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value VARCHAR NOT NULL,
    update_ts timestamp NOT NULL unique
);

CREATE TABLE IF NOT EXISTS stg.couriers (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value VARCHAR NOT NULL,
    update_ts timestamp NOT NULL unique
);

CREATE TABLE IF NOT EXISTS stg.deliveries (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL unique
);

-- CDM --

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id INT GENERATED ALWAYS AS IDENTITY,
    courier_id INT NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year INT NOT NULL,
    settlement_month INT NOT NULL CHECK("settlement_month" >= 1 AND "settlement_month" <= 12),
    orders_count INT NOT NULL CHECK("orders_count" >= 0),
    orders_total_sum real NOT NULL CHECK("orders_total_sum" >= 0),
    rate_avg NUMERIC(3,2) NOT NULL CHECK("rate_avg" >= 1 AND "rate_avg" <= 5),
    order_processing_fee real NOT NULL CHECK("order_processing_fee" >= 0),
    courier_order_sum real NOT NULL CHECK("courier_order_sum" >= 0),
    courier_tips_sum real NOT NULL CHECK("courier_tips_sum" >= 0),
    courier_reward_sum real NOT null CHECK("courier_reward_sum" >= 0),
    
  	constraint dm_courier_ledger_unique UNIQUE(courier_id, settlement_year, settlement_month)
);
-- DDS --

CREATE TABLE IF NOT EXISTS dds.couriers(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.calendar(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL CHECK("month" >= 1 AND "month" <= 12),
    day INT NOT NULL CHECK("day" >= 1 AND "day" <= 31),
    "date" date NOT NULL,
    "timestamp" TIMESTAMP NOT NULL UNIQUE

);

CREATE TABLE IF NOT EXISTS dds.orders(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    calendar_id INT NOT NULL REFERENCES dds.calendar(id),
    "sum" real NOT NULL CHECK ("sum" >= 0)
);

CREATE TABLE IF NOT EXISTS dds.deliveries(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    order_id INT NOT NULL REFERENCES dds.orders(id),
    address TEXT NOT NULL,
    courier_id INT NOT NULL REFERENCES dds.couriers(id),
    rate INT CHECK ("rate" >= 1 AND "rate" <=5),
    calendar_id INT NOT NULL REFERENCES dds.calendar(id),
    tip_sum real NOT NULL CHECK ("tip_sum" >= 0)
);
