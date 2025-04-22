/* Events based model which ignores relational modeling, so that all quotes and policies are included in calculations */

{{ config(
    materialized='table',
    tags=['fct', 'lifecycle', 'insurance']
) }}

WITH int_policies AS (
  SELECT 
    COALESCE(s.application_id, 'missing_application_id') AS application_id,
    p.policy_id,
    NULL AS quote_id,
    p.policy_created_at AS event_date,
    'policy_bound' AS event_type,
    NULL AS quote_count,
    1 AS policy_count,
    s.state,
    s.industry
  FROM {{ ref('stg_policies') }} p
  LEFT JOIN {{ ref('fct_quotes') }} q
    ON p.quote_id = q.quote_id
  LEFT JOIN {{ ref('fct_submissions') }} s
    ON q.application_id = s.application_id
),

int_quotes AS (
  SELECT 
    COALESCE(s.application_id, 'missing_application_id') AS application_id,
    NULL AS policy_id,
    COALESCE(q.quote_id, 'missing_quote_id') AS quote_id,
    q.quoted_date AS event_date,
    'quote_sent' AS event_type,
    1 AS quote_count,
    NULL AS policy_count,
    s.state,
    s.industry
  FROM {{ ref('fct_quotes') }} q
  LEFT JOIN {{ ref('fct_submissions') }} s
    ON q.application_id = s.application_id
),

lifecycle_union AS (
  SELECT * FROM int_policies
  UNION ALL
  SELECT * FROM int_quotes
),

-- Cross join calendar lookup to bring all dates into the model
calendar AS (
  SELECT date AS event_date
  FROM {{ ref('dim_dates') }}
),

-- Join date spine to events
final_events AS (
  SELECT 
    c.event_date,
    le.event_type,
    le.application_id,
    le.quote_id,
    le.policy_id,
    le.quote_count,
    le.policy_count,
    le.state,
    le.industry   
  FROM calendar c
  LEFT JOIN lifecycle_union le
    ON le.event_date = c.event_date
)

SELECT * FROM final_events
ORDER BY event_date
