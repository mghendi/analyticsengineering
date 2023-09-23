select 
review::int, 
product_id::int 
from {{ source ('staging', 'reviews')}}