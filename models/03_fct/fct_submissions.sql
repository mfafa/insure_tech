{{ config(materialized='table') }}

WITH ranked_submissions AS (
  SELECT
    application_id,
    state,
    industry,
    -- sample_json, -- remove as this is in an int_mart
    submission_date,
    __loaded_at,
    __file_name,
    __dbt_processed_at,
    TRY_TO_DATE(updated_at)::DATE AS updated_at
  FROM {{ ref('stg_submissions') }}
),

agg_updates AS (
  SELECT
    application_id,
    MIN(updated_at) AS __first_updated_at,
    MAX(updated_at) AS __last_updated_at
  FROM ranked_submissions
  GROUP BY application_id
),

latest_submission AS (
  SELECT
    rs.*
  FROM ranked_submissions rs
  JOIN (
    SELECT
      application_id,
      MAX(updated_at) AS max_update
    FROM ranked_submissions
    GROUP BY application_id
  ) latest ON rs.application_id = latest.application_id
           AND rs.updated_at = latest.max_update
)

SELECT
  ls.application_id,
  ls.state,
  ls.industry,
--   ls.sample_json,
  ls.submission_date,
  agg.__first_updated_at,
  agg.__last_updated_at,
  ls.__loaded_at,
  ls.__file_name,
  ls.__dbt_processed_at
FROM latest_submission ls
JOIN agg_updates agg
  ON ls.application_id = agg.application_id
