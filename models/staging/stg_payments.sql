SELECT 
    "ID" AS payment_id,
    "ORDERID" AS order_id,
    "PAYMENTMETHOD" AS payment_method,
    "STATUS" AS payment_status,
    "AMOUNT" AS amount,
    "CREATED" AS created_at
FROM {{ source('shop', 'payments')}}