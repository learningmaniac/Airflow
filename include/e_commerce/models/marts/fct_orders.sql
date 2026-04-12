with customers as (
    select
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ ref('stg__customers') }}
),
orders as (
    select 
        order_id,
        customer_id,
        order_status,
        order_purchase_date,
        order_approved_date,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from {{ ref('stg__orders') }}
),
order_payments as (
    select 
        order_id,
        payment_method,
        payment_installments,
        payment_value
    from {{ ref('stg__order_payments') }}
),
payments as (
    select 
        o.order_id,
        sum(payment_value) as total_payment_values,
        min(payment_value) as min_payment_values,
        max(payment_value) as max_payment_values,
        count(*) as total_count_of_payment_values,
        avg(payment_value) as avg_payment_values,
        listagg(payment_method, ', ') as payment_methods
    from orders o
    left join order_payments op on o.order_id = op.order_id
    group by o.order_id
),
fct_orders as (
    select 
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_date,
        o.order_approved_date,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        p.payment_methods,
        p.total_payment_values,
        p.min_payment_values,
        p.max_payment_values,
        p.total_count_of_payment_values,
        p.avg_payment_values
    from orders o
    left join payments p on o.order_id = p.order_id
)
select * from fct_orders