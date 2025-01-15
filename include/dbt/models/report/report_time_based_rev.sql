
-- Calculate the sales of the most recent day, week, month, and year.
-- Because this project works with old data, the latest day must be found.
WITH latest_date AS (
    SELECT MAX(dt.datetime) as max_date
    FROM {{ ref('fct_invoices') }} fi
    JOIN {{ ref('dim_datetime') }} dt 
        ON fi.datetime_id = dt.datetime_id
)
SELECT 
    'Past Week' as time_period,
    COUNT(DISTINCT fi.invoice_id) as num_transactions,
    SUM(fi.total) as total_revenue,
    COUNT(DISTINCT dc.customer_id) as unique_customers
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_datetime') }} dt 
    ON fi.datetime_id = dt.datetime_id
JOIN {{ ref('dim_customer') }} dc 
    ON fi.customer_id = dc.customer_id
CROSS JOIN latest_date ld
WHERE dt.datetime >= DATE_SUB(ld.max_date, INTERVAL 7 DAY)

UNION ALL

SELECT 
    'Past Month' as time_period,
    COUNT(DISTINCT fi.invoice_id) as num_transactions,
    SUM(fi.total) as total_revenue,
    COUNT(DISTINCT dc.customer_id) as unique_customers
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_datetime') }} dt 
    ON fi.datetime_id = dt.datetime_id
JOIN {{ ref('dim_customer') }} dc 
    ON fi.customer_id = dc.customer_id
CROSS JOIN latest_date ld
WHERE dt.datetime >= DATE_SUB(ld.max_date, INTERVAL 30 DAY)

UNION ALL

SELECT 
    'Past Year' as time_period,
    COUNT(DISTINCT fi.invoice_id) as num_transactions,
    SUM(fi.total) as total_revenue,
    COUNT(DISTINCT dc.customer_id) as unique_customers
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_datetime') }} dt 
    ON fi.datetime_id = dt.datetime_id
JOIN {{ ref('dim_customer') }} dc 
    ON fi.customer_id = dc.customer_id
CROSS JOIN latest_date ld
WHERE dt.datetime >= DATE_SUB(ld.max_date, INTERVAL 365 DAY)

