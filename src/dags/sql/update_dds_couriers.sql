WITH sq AS(
SELECT 
	json_array_elements_text(object_value::json)::json -> '_id' AS id,
	json_array_elements_text(object_value::json)::json -> 'name' AS name
FROM stg.couriers
WHERE update_ts = '{{ds}}'
)
INSERT INTO dds.couriers(object_id,	"name")
SELECT 
    REPLACE(id::text, '"', ''),
    REPLACE(name::text, '"', '')
FROM sq
ON CONFLICT (object_id)
DO UPDATE SET name=EXCLUDED.name;

