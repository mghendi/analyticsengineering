-- Pivoting the data to generate the desired table structure
SELECT
    TO_DATE(year_num || '-' || month_of_the_year_num || '-01', 'YYYY-MM-DD') AS ingestion_date,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '01' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_jan,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '02' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_feb,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '03' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_mar,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '04' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_apr,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '05' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_may,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '06' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_jun,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '07' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_jul,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '08' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_aug,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '09' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_sep,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '10' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_oct,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '11' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_nov,
    COALESCE(SUM(CASE WHEN month_of_the_year_num = '12' THEN orders_on_holiday ELSE 0 END), 0) AS tt_order_hol_dec
FROM
    {{ref('public_holiday_orders')}}
GROUP BY
    year_num, month_of_the_year_num