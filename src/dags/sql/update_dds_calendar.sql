WITH sq1 AS(
SELECT 
	(json_array_elements_text(object_value::json)::json -> 'order_ts')::text    AS ts
FROM stg.deliveries
WHERE update_ts = '{{ds}}'
UNION
SELECT 
	(json_array_elements_text(object_value::json)::json -> 'delivery_ts')::text AS ts
FROM stg.deliveries
WHERE update_ts = '{{ds}}'
),
sq2 AS(
SELECT
    REPLACE(ts, '"', '')::TIMESTAMP AS timestamp
FROM sq1    
)
INSERT INTO dds.calendar (timestamp, date, year, month, day)
SELECT
    timestamp,
    timestamp::date AS date,
    EXTRACT(YEAR FROM timestamp),
    EXTRACT(MONTH FROM timestamp),
    EXTRACT(DAY FROM timestamp)
FROM sq2
ON CONFLICT(timestamp)
DO NOTHING; 