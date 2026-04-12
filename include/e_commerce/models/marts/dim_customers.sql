
with orders as (
    select 
        order_id,
        customer_id,
        order_status,
        order_purchase_date,
        order_approved_date
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
cust_agg as (
    select 
        o.customer_id,
        count(distinct o.order_id) as total_orders,
        count(distinct case when o.order_status = 'delivered' then o.order_id end) as total_delivered_orders,
        count(distinct case when o.order_status = 'canceled' then o.order_id end) as total_canceled_orders,
        count(distinct case when o.order_status = 'shipped' then o.order_id end) as total_shipped_orders,
        count(distinct case when op.payment_method = 'credit_card' then o.order_id end) as total_credit_card_orders,
        count(distinct case when op.payment_method = 'boleto' then o.order_id end) as total_boleto_orders,
        count(distinct case when op.payment_method = 'voucher' then o.order_id end) as total_voucher_orders,
        count(distinct case when op.payment_method = 'debit_card' then o.order_id end) as total_debit_card_orders,
        count(distinct case when op.payment_method = 'not_defined' then o.order_id end) as total_not_defined_orders,
        sum(op.payment_value) as lifetime_payment_value,
        avg(op.payment_value) as avg_payment_value,
        min(op.payment_value) as min_payment_value,
        max(op.payment_value) as max_payment_value,
        min(o.order_purchase_date) as first_order_date,
        max(o.order_purchase_date) as last_order_date  
    from orders o
    left join order_payments op on o.order_id = op.order_id
    group by o.customer_id
)
,dim_customers as (
    select 
        c.customer_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        ca.total_orders,
        ca.total_delivered_orders,
        ca.total_canceled_orders,
        ca.total_shipped_orders,
        ca.total_credit_card_orders,
        ca.total_boleto_orders,
        ca.total_voucher_orders,
        ca.total_debit_card_orders,
        ca.total_not_defined_orders,
        ca.lifetime_payment_value,
        ca.avg_payment_value,
        ca.min_payment_value,
        ca.max_payment_value,
        ca.first_order_date,
        ca.last_order_date
    from {{ ref('stg__customers') }} c
    left join cust_agg ca on c.customer_id = ca.customer_id
)
select * from dim_customers