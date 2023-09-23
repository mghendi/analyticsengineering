SELECT
    CURRENT_DATE AS ingestion_date,
    (SELECT COUNT(*) FROM {{ ref ('int_late_shipments') }}) AS total_late_shipments,
    (SELECT COUNT(*) FROM {{ ref ('int_undelivered_shipments') }}) AS total_undelivered_shipments