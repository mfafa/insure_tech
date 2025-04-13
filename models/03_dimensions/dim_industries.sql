{{ config(
    materialized='incremental',
    unique_key='industry'
) }}

SELECT DISTINCT
  industry,
  UPPER(REPLACE(industry, ' ', '_')) AS industry_slug
FROM {{ ref('stg_submissions') }}
WHERE industry IS NOT NULL
{% if is_incremental() %}
  AND industry NOT IN (
    SELECT industry FROM {{ this }}
  )
{% endif %}
