@'
{{ config(materialized='table', tags=['marts']) }}

{{ generate_date_dimension() }}
'@ | Out-File -FilePath "medical_warehouse\models\marts\dim_dates.sql" -Encoding UTF8