
-- Find sales data over the most recent week.
WITH latest_date AS (
    SELECT MAX(dt.datetime) as max_date
    FROM {{ ref('fct_invoices') }} fi
    JOIN {{ ref('dim_datetime') }} dt 
        ON fi.datetime_id = dt.datetime_id
)
SELECT 
    fi.invoice_id,
    dt.datetime,
    dc.country,
    dp.description,
    fi.quantity,
    fi.total
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_datetime') }} dt 
    ON fi.datetime_id = dt.datetime_id
JOIN {{ ref('dim_customer') }} dc 
    ON fi.customer_id = dc.customer_id
JOIN {{ ref('dim_product') }} dp 
    ON fi.product_id = dp.product_id
CROSS JOIN latest_date ld
WHERE dt.datetime >= DATE_SUB(ld.max_date, INTERVAL 7 DAY)
ORDER BY dt.datetime DESC

