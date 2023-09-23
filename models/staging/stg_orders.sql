select 
order_id::int,
customer_id::int,
order_date::date,
product_id::varchar,
unit_price::int,
quantity::int,
amount::int
from {{ source ('staging', 'orders')}}