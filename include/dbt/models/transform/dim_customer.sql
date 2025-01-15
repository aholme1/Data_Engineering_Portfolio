
-- CTE to generate a unique customer_id using CustomerID and Country, and filter out rows with null CustomerID
-- Use a static country iso table in the datawarehouse to add standard country iso.
WITH customer_cte AS (
	SELECT DISTINCT
	    {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} as customer_id,
	    Country AS country
	FROM {{ source('retail', 'raw_invoices') }}
	WHERE CustomerID IS NOT NULL
)
SELECT
    t.*, -- Select all columns from the CTE (customer_id and country)
	cm.iso
FROM customer_cte t
LEFT JOIN {{ source('retail', 'country') }} cm ON t.country = cm.nicename -- Left join on country table using country names