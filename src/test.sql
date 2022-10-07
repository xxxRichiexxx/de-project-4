
INSERT INTO stg.restaurants (object_value, update_ts)
SELECT object_value, update_ts FROM tests.stg_restaurants
UNION ALL
SELECT object_value, update_ts FROM tests.stg_restaurants;

INSERT INTO stg.couriers (object_value, update_ts)
SELECT object_value, update_ts FROM tests.stg_couriers
UNION ALL
SELECT object_value, update_ts FROM tests.stg_couriers;

INSERT INTO stg.deliveries (object_value, update_ts)
SELECT object_value, update_ts FROM tests.stg_deliveries
UNION ALL
SELECT object_value, update_ts FROM tests.stg_deliveries;

INSERT INTO cdm.dm_courier_ledger
(courier_id, courier_name, settlement_year, settlement_month,
orders_count, orders_total_sum, rate_avg, order_processing_fee,
courier_order_sum, courier_tips_sum, courier_reward_sum)
SELECT
courier_id, courier_name, settlement_year, settlement_month,
orders_count, orders_total_sum, rate_avg, order_processing_fee,
courier_order_sum, courier_tips_sum, courier_reward_sum
FROM tests.dm_courier_ledger
UNION all
SELECT
courier_id, courier_name, settlement_year, settlement_month,
orders_count, orders_total_sum, rate_avg, order_processing_fee,
courier_order_sum, courier_tips_sum, courier_reward_sum
FROM tests.dm_courier_ledger;

INSERT INTO dds.couriers (object_id, "name")
SELECT object_id, "name" FROM tests.dds_couriers
UNION ALL
SELECT object_id, "name" FROM tests.dds_couriers;

INSERT INTO dds.calendar ("year", "month", "day", "date","timestamp")
SELECT "year", "month", "day", "date","timestamp" FROM tests.dds_calendar
UNION ALL
SELECT "year", "month", "day", "date","timestamp" FROM tests.dds_calendar;

INSERT INTO dds.orders (object_id, calendar_id, sum)
SELECT object_id, calendar_id, sum FROM tests.dds_orders
UNION ALL
SELECT object_id, calendar_id, sum FROM tests.dds_orders;

INSERT INTO tests.dds_deliveries (object_id, order_id, address, courier_id, rate, calendar_id, tip_sum) VALUES
('test', 7, 'Ул. Советская 3 кв. 422', 458,	5, 824,	255.0);

INSERT INTO dds.deliveries (object_id, order_id, address, courier_id, rate, calendar_id, tip_sum)
SELECT object_id, order_id, address, courier_id, rate, calendar_id, tip_sum FROM tests.dds_deliveries
UNION ALL
SELECT object_id, order_id, address, courier_id, rate, calendar_id, tip_sum FROM tests.dds_deliveries;
