-- Test to ensure all channels are classified
SELECT 
    channel_name,
    channel_type
FROM {{ ref('dim_channels') }}
WHERE channel_type IS NULL
   OR channel_type = ''

{% if target.name == 'dev' %}
LIMIT 10
{% endif %}