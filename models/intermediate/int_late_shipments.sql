SELECT
    o.order_id,
    o.order_date,
    sd.shipment_date,
    sd.delivery_date
FROM
    {{ ref ('stg_orders') }} o
JOIN
    {{ ref ('stg_shipment_deliveries') }} sd ON o.order_id = sd.order_id
WHERE
    sd.shipment_date >= o.order_date + INTERVAL '6 days'
    AND sd.delivery_date IS NULL