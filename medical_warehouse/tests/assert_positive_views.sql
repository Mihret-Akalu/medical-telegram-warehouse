@'
-- Test: Ensure view counts are non-negative
SELECT 
    message_id,
    channel_name,
    views
FROM {{ ref('stg_telegram_messages') }}
WHERE views < 0
  AND data_quality_status = 'valid'
'@ | Out-File -FilePath "medical_warehouse\tests\assert_positive_views.sql" -Encoding UTF8