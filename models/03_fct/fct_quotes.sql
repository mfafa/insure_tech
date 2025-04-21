{{ config(materialized='table') }}

SELECT 
  q.quote_id,
  q.application_id,
  q.quoted_date,

  -- Flags and metrics
  DATEDIFF('day', q.quoted_date, p.policy_created_at) AS _days_to_policy_bound,
  CASE WHEN p.policy_created_at IS NOT NULL THEN TRUE ELSE FALSE END AS _policy_created_flag,
  CASE 
    WHEN p.policy_id IS NULL 
         AND DATEDIFF('day', q.quoted_date, CURRENT_DATE()) > 30 
    THEN TRUE 
    ELSE FALSE 
  END AS _quote_expired, -- quotes with policies exist that have been bounded after 30 days, this logic needs to be validated

  -- Metadata fields
  q.__loaded_at,
  q.__file_name,
  CURRENT_TIMESTAMP AS __dbt_processed_at

FROM {{ ref('stg_quotes') }} q
LEFT JOIN {{ ref('stg_policies') }} p
  ON q.quote_id = p.quote_id
