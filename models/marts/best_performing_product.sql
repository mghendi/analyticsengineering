-- Calculating the total review points and percentage distribution of review points
WITH review_summary AS (
    SELECT
        p.product_name,
        SUM(r.review) AS tt_review_points,
        COUNT(*) FILTER (WHERE r.review = 1) AS one_star_reviews,
        COUNT(*) FILTER (WHERE r.review = 2) AS two_star_reviews,
        COUNT(*) FILTER (WHERE r.review = 3) AS three_star_reviews,
        COUNT(*) FILTER (WHERE r.review = 4) AS four_star_reviews,
        COUNT(*) FILTER (WHERE r.review = 5) AS five_star_reviews
    FROM
        {{ ref ('stg_reviews') }} r
    JOIN
        {{ source ('if_common', 'dim_products') }} p ON r.product_id = p.product_id
    GROUP BY
        p.product_name
),

-- Calculating the most ordered day for each product
most_ordered_day_summary AS (
    SELECT
        p.product_name,
        o.order_date AS most_ordered_day
    FROM
        {{ ref ('stg_orders') }} o
    JOIN
        {{ source ('if_common', 'dim_products') }} p ON o.product_id::int = p.product_id
    WHERE
        o.order_date IN (
            SELECT
                order_date
            FROM
                {{ ref ('stg_orders') }}
            WHERE
                product_id::int = p.product_id
            GROUP BY
                order_date
            ORDER BY
                COUNT(*) DESC
            LIMIT 1
        )
),

-- Determining if the most ordered day was a public holiday
most_ordered_day_public_holiday AS (
    SELECT
        m.product_name,
        m.most_ordered_day,
        CASE
            WHEN d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = FALSE THEN TRUE
            ELSE FALSE
        END AS is_public_holiday
    FROM
        most_ordered_day_summary m
    JOIN
        {{ source ('if_common', 'dim_dates') }} d ON m.most_ordered_day = d.calendar_dt
),

-- CTE to calculate the percentage distribution of early and late shipments for each product
shipment_summary AS (
    SELECT
        p.product_name,
        COUNT(*) FILTER (WHERE s.shipment_date >= o.order_date + INTERVAL '6 days') AS late_shipments,
        COUNT(*) FILTER (WHERE s.shipment_date < o.order_date + INTERVAL '6 days') AS early_shipments
    FROM
        {{ ref ('stg_shipment_deliveries') }} s 
    JOIN
       {{ ref ('stg_orders') }} o ON s.order_id = o.order_id
    JOIN
        {{ source ('if_common', 'dim_products') }} p ON o.product_id::int = p.product_id
    GROUP BY
        p.product_name
)

-- Combining the results
SELECT
    CURRENT_DATE AS ingestion_date,
    rs.product_name,
    modp.most_ordered_day,
    COALESCE(mop.is_public_holiday, FALSE) AS is_public_holiday,
    rs.tt_review_points,
    rs.one_star_reviews * 100.0 / rs.tt_review_points AS pct_one_star_review,
    rs.two_star_reviews * 100.0 / rs.tt_review_points AS pct_two_star_review,
    rs.three_star_reviews * 100.0 / rs.tt_review_points AS pct_three_star_review,
    rs.four_star_reviews * 100.0 / rs.tt_review_points AS pct_four_star_review,
    rs.five_star_reviews * 100.0 / rs.tt_review_points AS pct_five_star_review,
    ss.early_shipments * 100.0 / (ss.early_shipments + ss.late_shipments) AS pct_early_shipments,
    ss.late_shipments * 100.0 / (ss.early_shipments + ss.late_shipments) AS pct_late_shipments
FROM
    review_summary rs
JOIN
    most_ordered_day_summary modp ON rs.product_name = modp.product_name
LEFT JOIN
    most_ordered_day_public_holiday mop ON rs.product_name = mop.product_name AND modp.most_ordered_day = mop.most_ordered_day
JOIN
    shipment_summary ss ON rs.product_name = ss.product_name