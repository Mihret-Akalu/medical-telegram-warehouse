-- Test to ensure message length is reasonable
SELECT 
    message_id,
    channel_name,
    message_length
FROM {{ ref('stg_telegram_messages') }}
WHERE message_length < 0 OR message_length > 10000  -- Assuming max 10k characters

{% if target.name == 'dev' %}
LIMIT 10
{% endif %}