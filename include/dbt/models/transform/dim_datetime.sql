
--CTE to process different Date time formats, handle 'MM/DD/YYYY HH:MM' and 'MM/DD/YY HH:MM' format. Set data entry errors to null.
--Output table with year, month, day, hour, minute, and weekday to a unique row. Extracted from raw Datetime.
WITH datetime_cte AS (  
  SELECT DISTINCT
    InvoiceDate AS datetime_id,
    CASE
      -- handle 'MM/DD/YYYY HH:MM' case
      WHEN LENGTH(InvoiceDate) = 16 THEN
        PARSE_DATETIME('%m/%d/%Y %H:%M', InvoiceDate)
        -- handle 'MM/DD/YY HH:MM' case
      WHEN LENGTH(InvoiceDate) <= 14 THEN
        PARSE_DATETIME('%m/%d/%y %H:%M', InvoiceDate)
      ELSE
       --set data entry errors to null
        NULL
    END AS date_part,
  FROM {{ source('retail', 'raw_invoices') }}
  WHERE InvoiceDate IS NOT NULL
)

-- Extract individual date time components
SELECT
  datetime_id,
  date_part as datetime,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  EXTRACT(DAY FROM date_part) AS day,
  EXTRACT(HOUR FROM date_part) AS hour,
  EXTRACT(MINUTE FROM date_part) AS minute,
  EXTRACT(DAYOFWEEK FROM date_part) AS weekday
FROM datetime_cte