@'
-- Test: Ensure no messages have future dates
SELECT 
    message_id,
    channel_name,
    message_date
FROM {{ ref('stg_telegram_messages') }}
WHERE message_date > CURRENT_TIMESTAMP
  AND data_quality_status = 'valid'
'@ | Out-File -FilePath "medical_warehouse\tests\assert_no_future_messages.sql" -Encoding UTF8