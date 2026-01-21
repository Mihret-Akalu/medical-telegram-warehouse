@'
{{ config(materialized='table', tags=['marts']) }}

WITH channel_stats AS (
    SELECT 
        channel_name,
        channel_username,
        channel_title,
        MIN(message_date) as first_post_date,
        MAX(message_date) as last_post_date,
        COUNT(*) as total_posts,
        AVG(views) as avg_views,
        AVG(forwards) as avg_forwards,
        SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as posts_with_media,
        SUM(CASE WHEN has_image THEN 1 ELSE 0 END) as posts_with_image
    FROM {{ ref('stg_telegram_messages') }}
    WHERE data_quality_status = 'valid'
    GROUP BY 1, 2, 3
),

channel_classification AS (
    SELECT 
        *,
        -- Classify channel type
        CASE
            {% for keyword in var('pharmaceutical_keywords') %}
            WHEN LOWER(channel_name) LIKE '%{{ keyword }}%' THEN 'Pharmaceutical'
            {% endfor %}
            
            {% for keyword in var('cosmetics_keywords') %}
            WHEN LOWER(channel_name) LIKE '%{{ keyword }}%' THEN 'Cosmetics'
            {% endfor %}
            
            {% for keyword in var('medical_keywords') %}
            WHEN LOWER(channel_name) LIKE '%{{ keyword }}%' THEN 'Medical'
            {% endfor %}
            
            ELSE 'Other'
        END as channel_type
    FROM channel_stats
),

enriched_channels AS (
    SELECT 
        -- Surrogate key
        ROW_NUMBER() OVER (ORDER BY total_posts DESC) as channel_key,
        
        -- Channel identifiers
        channel_name,
        channel_username,
        channel_title,
        channel_type,
        
        -- Activity metrics
        first_post_date,
        last_post_date,
        total_posts,
        
        -- Engagement metrics
        ROUND(avg_views, 2) as avg_views,
        ROUND(avg_forwards, 2) as avg_forwards,
        
        -- Content metrics
        posts_with_media,
        posts_with_image,
        ROUND(posts_with_media * 100.0 / NULLIF(total_posts, 0), 2) as media_percentage,
        ROUND(posts_with_image * 100.0 / NULLIF(total_posts, 0), 2) as image_percentage,
        
        -- Derived metrics
        CASE 
            WHEN last_post_date >= DATE('now', '-7 days') 
            THEN 'active' 
            WHEN last_post_date >= DATE('now', '-30 days') 
            THEN 'moderate' 
            ELSE 'inactive' 
        END as activity_status
        
    FROM channel_classification
)

SELECT *
FROM enriched_channels
ORDER BY channel_key
'@ | Out-File -FilePath "medical_warehouse\models\marts\dim_channels.sql" -Encoding UTF8