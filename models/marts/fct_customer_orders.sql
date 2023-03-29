-- Imports
WITH orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

payments AS (

    SELECT * FROM {{ ref('stg_payments') }}

),

customers AS (

    SELECT * FROM {{ ref('stg_customers') }}

),

-- Logical CTEs
succeeded_payments_per_order AS (
    SELECT order_id, MAX(created_at) AS payment_finalized_date, SUM(amount) AS total_amount_paid
    FROM payments
    WHERE payment_status <> 'fail'
    GROUP BY 1
),

paid_orders AS (
    SELECT orders.order_id, orders.customer_id, orders.order_date AS order_placed_at,
    orders.order_status, p.total_amount_paid, p.payment_finalized_date, C.first_name AS customer_first_name,
    C.last_name AS customer_last_name
    FROM orders
    LEFT JOIN succeeded_payments_per_order p ON orders.order_id = p.order_id
    LEFT JOIN customers C ON orders.customer_id = C.customer_id
),

customer_orders AS (
    SELECT C.customer_id, MIN(order_date) AS first_order_date, MAX(order_date) AS most_recent_order_date,
    COUNT(orders.order_id) AS number_of_orders
    FROM customers C 
    LEFT JOIN orders AS orders ON orders.customer_id = C.customer_id
    GROUP BY 1
),

paid_customers AS (
    SELECT p.order_id, SUM(t2.total_amount_paid) AS clv_bad
    FROM paid_orders p
    LEFT JOIN paid_orders t2 ON p.customer_id = t2.customer_id AND p.order_id >= t2.order_id
    GROUP BY 1
    ORDER BY p.order_id
),

-- Final CTEs
final AS (
    SELECT p.*, ROW_NUMBER() OVER (ORDER BY p.order_id) AS transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) AS customer_sales_seq,
    CASE 
        WHEN c.first_order_date = p.order_placed_at
        THEN 'new'
        ELSE 'return'
    END AS nvsr, x.clv_bad AS customer_lifetime_value, c.first_order_date AS fdos
    FROM paid_orders p
    LEFT JOIN customer_orders AS c USING (customer_id)
    LEFT OUTER JOIN paid_customers x ON x.order_id = p.order_id
    ORDER BY order_id
)

SELECT * FROM final