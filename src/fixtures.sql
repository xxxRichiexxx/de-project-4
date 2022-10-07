-- Таблицы с эталонным результатом
CREATE SCHEMA IF NOT EXISTS tests;

CREATE TABLE IF NOT EXISTS tests.stg_restaurants (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value VARCHAR NOT NULL,
    update_ts timestamp NOT NULL unique
);

INSERT INTO tests.stg_restaurants (object_value, update_ts) VALUES
('[{"_id": "test"}]', '2022-10-04 00:00:00.000');


CREATE TABLE IF NOT EXISTS tests.stg_couriers (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value VARCHAR NOT NULL,
    update_ts timestamp NOT NULL unique
);

INSERT INTO tests.stg_couriers (object_value, update_ts) VALUES
('[{"_id": "test", "name"}]', '2022-10-04 00:00:00.000');


CREATE TABLE IF NOT EXISTS tests.stg_deliveries (
    id INT GENERATED ALWAYS AS IDENTITY,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL unique
);

INSERT INTO tests.stg_deliveries (object_value, update_ts) VALUES
('[{"order_id": "test"}]', '2022-10-04 00:00:00.000');


CREATE TABLE IF NOT EXISTS tests.dm_courier_ledger (
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
    courier_reward_sum real NOT NULL
);

INSERT INTO tests.dm_courier_ledger 
(courier_id, courier_name, settlement_year, settlement_month,
orders_count, orders_total_sum, rate_avg, order_processing_fee,
courier_order_sum, courier_tips_sum, courier_reward_sum) VALUES
(401, 'test', 2022,	11,	12,	54445.0, 5.00, 13611.25, 5444.5, 8161.0, 13197.45);

CREATE TABLE IF NOT EXISTS tests.dds_couriers(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL
);

INSERT INTO tests.dds_couriers(object_id, "name") VALUES
('test',	'Евгения Орлова');

CREATE TABLE IF NOT EXISTS tests.dds_calendar(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    "date" date NOT NULL,
    "timestamp" TIMESTAMP NOT NULL UNIQUE

);

INSERT INTO tests.dds_calendar("year", "month", "day", "date","timestamp") VALUES
(2022, 10, 4, '2022-10-04', '2022-10-04 03:34:26.733');


CREATE TABLE IF NOT EXISTS tests.dds_orders(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    calendar_id INT NOT NULL REFERENCES dds.calendar(id),
    "sum" real NOT NULL
);

INSERT INTO tests.dds_orders (object_id, calendar_id, sum) VALUES
('test', 990, 978.0);

CREATE TABLE IF NOT EXISTS tests.dds_deliveries(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    order_id INT NOT NULL REFERENCES dds.orders(id),
    address TEXT NOT NULL,
    courier_id INT NOT NULL REFERENCES dds.couriers(id),
    rate INT,
    calendar_id INT NOT NULL REFERENCES dds.calendar(id),
    tip_sum real NOT NULL
);

INSERT INTO tests.dds_deliveries (object_id, order_id, address, courier_id, rate, calendar_id, tip_sum) VALUES
('test', 7, 'Ул. Советская 3 кв. 422', 458,	5, 824,	255.0);