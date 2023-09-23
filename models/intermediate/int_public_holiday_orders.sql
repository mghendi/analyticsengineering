-- Calculating the total number of orders placed on public holidays
SELECT
    dd.year_num,
    dd.month_of_the_year_num,
    SUM(CASE WHEN dd.day_of_the_week_num >= 1 AND dd.day_of_the_week_num <= 5 AND dd.working_day = FALSE THEN 1 ELSE 0 END) AS orders_on_holiday
FROM
    {{ source ('if_common', 'dim_dates') }} dd
JOIN
    {{ ref ('stg_orders') }} o ON dd.calendar_dt = o.order_date
WHERE
    dd.calendar_dt >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'
    AND dd.calendar_dt < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY
    dd.year_num, dd.month_of_the_year_num