/* Builds a list of all unique json keys */

{{ config(
    materialized='incremental',
    unique_key='json_key'
) }}

WITH new_keys AS (
  SELECT DISTINCT
    json_key
  FROM {{ ref('int_submissions__sample_json_flattened_l1') }}
  WHERE json_key IS NOT NULL
  {% if is_incremental() %}
    AND json_key NOT IN (
      SELECT json_key FROM {{ this }}
    )
  {% endif %}
)

SELECT
  json_key
FROM new_keys

