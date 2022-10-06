DELETE FROM cdm.dm_courier_ledger
WHERE settlement_year = {{execution_date.year}} AND settlement_month = {{execution_date.month}};

WITH sq AS(
    SELECT 
        c.id                                                             		AS courier_id,
        c.name                                                                  AS courier_name,
        EXTRACT( YEAR FROM cal.date + INTERVAL '1 MONTH')                       AS settlement_year,
        EXTRACT( MONTH FROM cal.date + INTERVAL '1 MONTH')                      AS settlement_month,
        COUNT(DISTINCT order_id)                                                AS orders_count,
        SUM(o.sum)                                                              AS orders_total_sum,
        AVG(d.rate)                                                             AS rate_avg,
        SUM(o.sum) * 0.25                                                       AS order_processing_fee,
        CASE
            WHEN AVG(d.rate) < 4 THEN 0.06*SUM(o.sum)
            WHEN AVG(d.rate) >=4 AND AVG(d.rate) < 4.5 THEN 0.07*SUM(o.sum)
            WHEN AVG(d.rate) >=4.5 AND AVG(d.rate) < 4.9 THEN 0.08*SUM(o.sum)
            WHEN AVG(d.rate) >= 4.9 THEN 0.1*SUM(o.sum)
        END                                                                     AS courier_order_sum,
        SUM(d.tip_sum)                                                          AS courier_tips_sum        
    FROM dds.deliveries                                                         AS d 
    JOIN dds.orders                                                             AS o 
        ON d.order_id = o.id
    JOIN dds.couriers                                                           AS c 
        ON d.courier_id = c.id 
    JOIN dds.calendar                                                           AS cal 
        ON o.calendar_id = cal.id
    WHERE cal.year = {{execution_date.year}} AND cal.month = {{execution_date.month}}
    GROUP BY c.id, courier_name, settlement_year, settlement_month
)
INSERT INTO cdm.dm_courier_ledger
    (courier_id, courier_name, settlement_year, settlement_month, orders_count,
    orders_total_sum, rate_avg, order_processing_fee, courier_order_sum,
    courier_tips_sum, courier_reward_sum)
SELECT
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    CASE
        WHEN rate_avg < 4 AND courier_order_sum < 100 THEN 100
        WHEN rate_avg >= 4 AND rate_avg < 4.5 AND courier_order_sum < 150 THEN 150
        WHEN rate_avg >= 4.5 AND rate_avg < 4.9 AND courier_order_sum < 175 THEN 175
        WHEN rate_avg >= 4.9 AND courier_order_sum < 200 THEN 200
        ELSE courier_order_sum
    END courier_order_sum,
    courier_tips_sum,
    courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM sq;

