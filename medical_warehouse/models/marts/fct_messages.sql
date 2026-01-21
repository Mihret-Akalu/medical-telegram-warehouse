@'
{{ config(materialized='table', tags=['marts']) }}

WITH stg_messages AS (
    SELECT 
        message_id,
        channel_name,
        message_date,
        cleaned_message_text as message_text,
        message_length,
        has_media,
        has_image,
        views,
        forwards,
        data_quality_status
    FROM {{ ref('stg_telegram_messages') }}
    WHERE data_quality_status = 'valid'
),

messages_with_keys AS (
    SELECT 
        m.message_id,
        c.channel_key,
        d.date_key,
        m.message_text,
        m.message_length,
        m.has_media,
        m.has_image,
        m.views as view_count,
        m.forwards as forward_count,
        m.data_quality_status
    FROM stg_messages m
    LEFT JOIN {{ ref('dim_channels') }} c 
        ON m.channel_name = c.channel_name
    LEFT JOIN {{ ref('dim_dates') }} d 
        ON DATE(m.message_date) = d.full_date
)

SELECT 
    message_id,
    channel_key,
    date_key,
    message_text,
    message_length,
    view_count,
    forward_count,
    has_image,
    data_quality_status
FROM messages_with_keys
WHERE channel_key IS NOT NULL
  AND date_key IS NOT NULL
'@ | Out-File -FilePath "medical_warehouse\models\marts\fct_messages.sql" -Encoding UTF8