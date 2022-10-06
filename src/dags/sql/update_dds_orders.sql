WITH sq1 AS(
SELECT 
	json_array_elements_text(object_value::json)::json -> 'order_id' AS order_id,
	json_array_elements_text(object_value::json)::json -> 'order_ts' AS order_ts,
    json_array_elements_text(object_value::json)::json -> 'sum'      AS sum
FROM stg.deliveries
WHERE update_ts = '{{ds}}'
)
INSERT INTO dds.orders (object_id, calendar_id, sum)
SELECT 
    REPLACE(order_id::text, '"', '')    AS object_id,
    c.id                                AS calendar_id,
    REPLACE(sum::text, '"', '')::real   AS sum
FROM sq1
JOIN dds.calendar AS c
    ON REPLACE(sq1.order_ts::text, '"', '')::TIMESTAMP = c.timestamp
ON CONFLICT (object_id)
DO UPDATE SET calendar_id=EXCLUDED.calendar_id, sum=EXCLUDED.sum;