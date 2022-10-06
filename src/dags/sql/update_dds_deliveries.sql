WITH sq1 AS(
SELECT 
	json_array_elements_text(object_value::json)::json -> 'order_id'    AS order_id,
	json_array_elements_text(object_value::json)::json -> 'delivery_ts' AS delivery_ts,
    json_array_elements_text(object_value::json)::json -> 'delivery_id' AS delivery_id,
    json_array_elements_text(object_value::json)::json -> 'address'     AS address,
    json_array_elements_text(object_value::json)::json -> 'courier_id'  AS courier_id,
    json_array_elements_text(object_value::json)::json -> 'rate'        AS rate,
    json_array_elements_text(object_value::json)::json -> 'tip_sum'     AS tip_sum
FROM stg.deliveries
WHERE update_ts = '{{ds}}'
)
INSERT INTO dds.deliveries (object_id, order_id, address, courier_id, rate, calendar_id, tip_sum)
SELECT
    REPLACE(delivery_id::text, '"', '') AS object_id,
    o.id                                AS order_id,
    REPLACE(sq1.address::text, '"', '') AS address,
    c.id                                AS courier_id,
    sq1.rate::text::int                 AS rate,
    cal.id                              AS calendar_id,
    sq1.tip_sum::text::real             AS tip_sum
FROM sq1
LEFT JOIN dds.orders AS o 
    ON REPLACE(sq1.order_id::text, '"', '') = o.object_id
LEFT JOIN dds.couriers AS c
    ON REPLACE(sq1.courier_id::text, '"', '') = c.object_id
LEFT JOIN dds.calendar AS cal 
    ON REPLACE(sq1.delivery_ts::text, '"', '')::timestamp = cal.timestamp
ON CONFLICT(object_id)
DO UPDATE SET
    order_id = EXCLUDED.order_id,
    address = EXCLUDED.address,
    courier_id = EXCLUDED.courier_id,
    rate = EXCLUDED.rate,
    calendar_id = EXCLUDED.calendar_id,
    tip_sum = EXCLUDED.tip_sum;