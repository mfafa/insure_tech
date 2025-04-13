{{ config(
    materialized='incremental',
    unique_key='state_code'
) }}

SELECT DISTINCT
  state AS state_code,
  INITCAP(state) AS state_name
FROM {{ ref('stg_submissions') }}
WHERE state IS NOT NULL
{% if is_incremental() %}
  AND state NOT IN (
    SELECT state_code FROM {{ this }}
  )
{% endif %}
