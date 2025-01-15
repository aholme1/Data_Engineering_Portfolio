
-- Check when business is most frequent. Produces the total revenue for each hour of the day across the year.
SELECT 
    dt.hour,
    COUNT(DISTINCT fi.invoice_id) as num_transactions,
    SUM(fi.total) as total_revenue,
    AVG(fi.total) as avg_transaction_value,
    COUNT(DISTINCT dc.customer_id) as unique_customers
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_datetime') }} dt 
    ON fi.datetime_id = dt.datetime_id
JOIN {{ ref('dim_customer') }} dc 
    ON fi.customer_id = dc.customer_id
GROUP BY dt.hour
ORDER BY dt.hour

