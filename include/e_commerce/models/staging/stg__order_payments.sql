select 
    order_id,
    payment_sequential,
    payment_type as payment_method,
    payment_installments,
    payment_value
from {{source('raw', 'order_payments')}}