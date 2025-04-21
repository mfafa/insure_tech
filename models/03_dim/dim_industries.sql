{{ config(
    materialized='incremental',
    unique_key='industry'
) }}

SELECT DISTINCT
  industry
  -- placeholder dim table, label / group industries at a later date
FROM {{ ref('stg_submissions') }}
WHERE industry IS NOT NULL
{% if is_incremental() %}
  AND industry NOT IN (
    SELECT industry FROM {{ this }}
  )
{% endif %}
