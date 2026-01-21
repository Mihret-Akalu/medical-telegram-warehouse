@'
{{ config(materialized='table', tags=['staging']) }}

WITH raw_data AS (
    SELECT 
        message_id,
        channel_name,
        channel_username,
        channel_title,
        message_date,
        message_text,
        has_media,
        image_path,
        COALESCE(views, 0) as views,
        COALESCE(forwards, 0) as forwards,
        scraped_at
    FROM {{ source('raw', 'telegram_messages') }}
    WHERE message_date IS NOT NULL
),

cleaned_data AS (
    SELECT 
        -- Message identifiers
        message_id,
        channel_name,
        channel_username,
        channel_title,
        
        -- Timestamps
        message_date,
        scraped_at,
        
        -- Message content
        message_text,
        TRIM(message_text) as cleaned_message_text,
        LENGTH(TRIM(message_text)) as message_length,
        
        -- Media information
        has_media,
        image_path,
        CASE 
            WHEN image_path IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as has_image,
        
        -- Engagement metrics
        views,
        forwards,
        
        -- Data quality flags
        CASE 
            WHEN message_text IS NULL OR TRIM(message_text) = '' 
            THEN TRUE ELSE FALSE 
        END as is_empty_message,
        
        CASE 
            WHEN message_date > CURRENT_TIMESTAMP 
            THEN TRUE ELSE FALSE 
        END as is_future_date,
        
        CASE 
            WHEN views < 0 THEN TRUE ELSE FALSE 
        END as has_negative_views
        
    FROM raw_data
)

SELECT 
    *,
    -- Add data quality summary
    CASE 
        WHEN is_empty_message OR is_future_date OR has_negative_views
        THEN 'needs_review' 
        ELSE 'valid' 
    END as data_quality_status
FROM cleaned_data
WHERE NOT is_future_date  -- Exclude future dates
'@ | Out-File -FilePath "medical_warehouse\models\staging\stg_telegram_messages.sql" -Encoding UTF8