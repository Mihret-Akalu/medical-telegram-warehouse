{{
    config(
        materialized='table',
        schema='marts',
        tags=['analytics', 'marts']
    )
}}

WITH product_mentions AS (
    SELECT 
        UNNEST(potential_products) as product_name,
        channel_key,
        date_key,
        views,
        forwards,
        engagement_score
    FROM {{ ref('fct_messages') }}
    WHERE potential_products IS NOT NULL
      AND ARRAY_LENGTH(potential_products, 1) > 0
),

product_stats AS (
    SELECT 
        LOWER(TRIM(product_name)) as product_name,
        COUNT(*) as mention_count,
        COUNT(DISTINCT channel_key) as channel_count,
        SUM(views) as total_views,
        AVG(views) as avg_views,
        SUM(forwards) as total_forwards,
        AVG(engagement_score) as avg_engagement,
        MIN(date_key) as first_mentioned_date,
        MAX(date_key) as last_mentioned_date
    FROM product_mentions
    WHERE product_name IS NOT NULL 
      AND TRIM(product_name) != ''
    GROUP BY 1
),

product_categories AS (
    SELECT 
        product_name,
        mention_count,
        channel_count,
        total_views,
        avg_views,
        total_forwards,
        avg_engagement,
        first_mentioned_date,
        last_mentioned_date,
        
        -- Categorize products
        CASE 
            WHEN product_name LIKE '%tablet%' OR product_name LIKE '%pill%' THEN 'Tablets'
            WHEN product_name LIKE '%capsule%' THEN 'Capsules'
            WHEN product_name LIKE '%cream%' OR product_name LIKE '%ointment%' THEN 'Topical'
            WHEN product_name LIKE '%syrup%' OR product_name LIKE '%liquid%' THEN 'Liquids'
            WHEN product_name LIKE '%injection%' OR product_name LIKE '%injectable%' THEN 'Injectables'
            WHEN product_name LIKE '%vitamin%' OR product_name LIKE '%supplement%' THEN 'Supplements'
            WHEN product_name LIKE '%device%' OR product_name LIKE '%equipment%' THEN 'Medical Devices'
            ELSE 'Other'
        END as product_category,
        
        -- Extract strength
        REGEXP_SUBSTR(product_name, '\d+\s*(mg|ml|g)') as strength,
        
        -- Popularity rank
        ROW_NUMBER() OVER (ORDER BY mention_count DESC) as popularity_rank,
        ROW_NUMBER() OVER (ORDER BY total_views DESC) as views_rank
        
    FROM product_stats
)

SELECT *
FROM product_categories
WHERE mention_count >= 1  -- At least mentioned once
ORDER BY mention_count DESC, total_views DESC