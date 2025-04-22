/* dim_states fact table, star schema, with additional context for grouping by region */

{{ config(
    materialized='incremental',
    unique_key='state'
) }}

SELECT DISTINCT
  state AS state,
  -- REGION
CASE
  WHEN state IN ('California', 'Oregon', 'Washington', 'Nevada', 'Hawaii', 'Alaska') THEN 'West'
  WHEN state IN ('Arizona', 'Colorado', 'Idaho', 'Montana', 'New Mexico', 'Utah', 'Wyoming') THEN 'Mountain'
  WHEN state IN ('North Dakota', 'South Dakota', 'Nebraska', 'Kansas', 'Minnesota', 'Iowa', 'Missouri') THEN 'Midwest'
  WHEN state IN ('Wisconsin', 'Illinois', 'Indiana', 'Michigan', 'Ohio') THEN 'Great Lakes'
  WHEN state IN ('Texas', 'Oklahoma', 'Arkansas', 'Louisiana') THEN 'South Central'
  WHEN state IN ('Mississippi', 'Alabama', 'Tennessee', 'Kentucky') THEN 'Southeast Central'
  WHEN state IN ('Georgia', 'Florida', 'South Carolina', 'North Carolina') THEN 'Southeast Coastal'
  WHEN state IN ('Virginia', 'West Virginia', 'Maryland', 'Delaware') THEN 'Mid-Atlantic'
  WHEN state IN ('Pennsylvania', 'New Jersey', 'New York', 'Connecticut', 'Rhode Island', 'Massachusetts', 'Vermont', 'New Hampshire', 'Maine') THEN 'Northeast'
  ELSE 'Unknown'
END AS region,

-- TIMEZONE
CASE
  WHEN state IN ('California', 'Oregon', 'Washington', 'Nevada') THEN 'Pacific'
  WHEN state IN ('Arizona', 'Colorado', 'Utah', 'Montana', 'Idaho', 'New Mexico', 'Wyoming') THEN 'Mountain'
  WHEN state IN ('Texas', 'Oklahoma', 'Kansas', 'Nebraska', 'South Dakota', 'North Dakota', 'Minnesota', 'Iowa', 'Missouri') THEN 'Central'
  WHEN state IN ('Wisconsin', 'Illinois', 'Indiana', 'Michigan', 'Ohio', 'Kentucky', 'Tennessee', 'Mississippi', 'Louisiana', 'Arkansas') THEN 'Central'
  WHEN state IN ('Georgia', 'South Carolina', 'North Carolina', 'Florida', 'Virginia', 'West Virginia', 'Maryland', 'Delaware', 'Pennsylvania', 'New Jersey', 'New York') THEN 'Eastern'
  WHEN state IN ('Connecticut', 'Rhode Island', 'Massachusetts', 'Vermont', 'New Hampshire', 'Maine') THEN 'Eastern'
  WHEN state = 'Alaska' THEN 'Alaska'
  WHEN state = 'Hawaii' THEN 'Hawaii-Aleutian'
  ELSE 'Unknown'
END AS timezone

FROM {{ ref('stg_submissions') }}
WHERE state IS NOT NULL
{% if is_incremental() %}
  AND state NOT IN (
    SELECT state FROM {{ this }}
  )
{% endif %}
