select 
shipment_id::int,
order_id::int,
shipment_date::date,
delivery_date::date 
from {{ source ('staging', 'shipment_deliveries')}}