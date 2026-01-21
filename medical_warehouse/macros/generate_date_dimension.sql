@'
{% macro generate_date_dimension() %}

WITH date_range AS (
    SELECT 
        date('{{ var("start_date", "2024-01-01") }}', '+' || (value-1) || ' days') as date
    FROM generate_series(1, (
        SELECT julianday('{{ var("end_date", "2026-12-31") }}') - 
               julianday('{{ var("start_date", "2024-01-01") }}') + 1
    ))
),

enriched_dates AS (
    SELECT
        -- Surrogate key
        CAST(strftime('%Y%m%d', date) AS INTEGER) as date_key,
        
        -- Date components
        date as full_date,
        CAST(strftime('%Y', date) AS INTEGER) as year,
        (CAST(strftime('%m', date) AS INTEGER) - 1) / 3 + 1 as quarter,
        CAST(strftime('%m', date) AS INTEGER) as month,
        
        CASE CAST(strftime('%m', date) AS INTEGER)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END as month_name,
        
        CAST(strftime('%W', date) AS INTEGER) + 1 as week_of_year,
        CAST(strftime('%d', date) AS INTEGER) as day_of_month,
        CAST(strftime('%w', date) AS INTEGER) as day_of_week,
        
        -- Day names
        CASE CAST(strftime('%w', date) AS INTEGER)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END as day_name,
        
        -- Special flags
        CASE 
            WHEN CAST(strftime('%w', date) AS INTEGER) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END as is_weekend
        
    FROM date_range
)

SELECT *
FROM enriched_dates
ORDER BY date_key

{% endmacro %}
'@ | Out-File -FilePath "medical_warehouse\macros\generate_date_dimension.sql" -Encoding UTF8