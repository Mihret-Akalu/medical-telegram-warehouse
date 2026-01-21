-- Sample Analytical Queries

-- 1. Top 10 Most Active Channels
SELECT 
    channel_name,
    channel_type,
    total_posts,
    avg_views,
    activity_status
FROM dim_channels
ORDER BY total_posts DESC
LIMIT 10;

-- 2. Daily Posting Trends
SELECT 
    d.full_date,
    d.day_name,
    COUNT(f.message_id) as post_count,
    SUM(f.view_count) as total_views,
    AVG(f.view_count) as avg_views_per_post
FROM fct_messages f
JOIN dim_dates d ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date DESC;

-- 3. Channel Performance by Type
SELECT 
    channel_type,
    COUNT(*) as channel_count,
    SUM(total_posts) as total_posts,
    ROUND(AVG(avg_views), 2) as avg_views_per_channel
FROM dim_channels
GROUP BY channel_type
ORDER BY total_posts DESC;

-- 4. Messages with Images vs Without
SELECT 
    CASE WHEN has_image = 1 THEN 'With Images' ELSE 'Text Only' END as message_type,
    COUNT(*) as message_count,
    AVG(view_count) as avg_views,
    AVG(forward_count) as avg_forwards
FROM fct_messages
GROUP BY has_image
ORDER BY message_count DESC;
