-- CTE to calculate the total number of orders placed on public holidays
WITH public_holiday_orders AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(CASE WHEN day_of_week >= 1 AND day_of_week <= 5 AND working_day = FALSE THEN 1 ELSE 0 END) AS orders_on_holiday
    FROM
        {{ source('staging', 'orders') }}
    WHERE
        order_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'
        AND order_date < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY
        DATE_TRUNC('month', order_date)
)

-- Pivot the data to generate the desired table structure
SELECT
    DATE_TRUNC('month', month) AS ingestion_date,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 1 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_jan,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 2 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_feb,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 3 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_mar,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 4 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_apr,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 5 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_may,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 6 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_jun,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 7 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_jul,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 8 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_aug,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 9 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_sep,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 10 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_oct,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 11 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_nov,
    COALESCE(SUM(CASE WHEN EXTRACT(MONTH FROM month) = 12 THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_dec
FROM
    public_holiday_orders
GROUP BY
    DATE_TRUNC('month', month)
ORDER BY
    ingestion_date;
