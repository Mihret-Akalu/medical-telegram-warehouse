{{
    config(
        materialized='table',
        schema='marts',
        tags=['analytics', 'marts']
    )
}}

WITH daily_stats AS (
    SELECT 
        c.channel_key,
        c.channel_name,
        c.channel_type,
        d.full_date,
        COUNT(*) as daily_posts,
        SUM(f.views) as daily_views,
        SUM(f.forwards) as daily_forwards,
        AVG(f.engagement_score) as avg_daily_engagement,
        SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) as daily_images
    FROM {{ ref('fct_messages') }} f
    JOIN {{ ref('dim_channels') }} c ON f.channel_key = c.channel_key
    JOIN {{ ref('dim_dates') }} d ON f.date_key = d.date_key
    GROUP BY 1, 2, 3, 4
),

weekly_stats AS (
    SELECT 
        channel_key,
        channel_name,
        channel_type,
        DATE_TRUNC('week', full_date) as week_start,
        SUM(daily_posts) as weekly_posts,
        SUM(daily_views) as weekly_views,
        SUM(daily_forwards) as weekly_forwards,
        AVG(avg_daily_engagement) as avg_weekly_engagement,
        SUM(daily_images) as weekly_images
    FROM daily_stats
    GROUP BY 1, 2, 3, 4
),

channel_summary AS (
    SELECT 
        c.channel_key,
        c.channel_name,
        c.channel_type,
        c.total_posts,
        c.avg_views,
        c.avg_forwards,
        c.media_percentage,
        c.image_percentage,
        c.activity_status,
        c.engagement_level,
        c.business_segment,
        
        -- Recent activity (last 7 days)
        COALESCE(SUM(CASE WHEN ds.full_date >= CURRENT_DATE - INTERVAL '7 days' 
                     THEN ds.daily_posts END), 0) as posts_last_7_days,
        
        -- Growth metrics
        CASE 
            WHEN COUNT(DISTINCT ws.week_start) >= 2 THEN
                (MAX(ws.weekly_posts) - MIN(ws.weekly_posts)) / NULLIF(MIN(ws.weekly_posts), 0) * 100
            ELSE 0
        END as weekly_growth_percentage,
        
        -- Content effectiveness
        CASE 
            WHEN c.total_posts > 0 THEN
                (c.avg_views * c.total_posts) / NULLIF(c.total_posts, 0)
            ELSE 0
        END as content_effectiveness_score
        
    FROM {{ ref('dim_channels') }} c
    LEFT JOIN daily_stats ds ON c.channel_key = ds.channel_key
    LEFT JOIN weekly_stats ws ON c.channel_key = ws.channel_key
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)

SELECT 
    *,
    
    -- Performance classification
    CASE 
        WHEN content_effectiveness_score > 1000 THEN 'High Performer'
        WHEN content_effectiveness_score > 100 THEN 'Medium Performer'
        ELSE 'Low Performer'
    END as performance_category,
    
    -- Recommendation
    CASE 
        WHEN image_percentage < 30 THEN 'Increase visual content'
        WHEN posts_last_7_days < 5 THEN 'Increase posting frequency'
        WHEN avg_views < 50 THEN 'Improve content quality'
        ELSE 'Maintain current strategy'
    END as improvement_recommendation
    
FROM channel_summary
ORDER BY content_effectiveness_score DESC