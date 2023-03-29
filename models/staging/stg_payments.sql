SELECT 
    "ID" AS payment_id,
    "ORDERID" AS order_id,
    "PAYMENTMETHOD" AS payment_method,
    "STATUS" AS payment_status,
    {{ cents_to_dollars('"AMOUNT"', 4) }} AS amount,
    "CREATED" AS created_at
FROM {{ source('shop', 'payments')}}