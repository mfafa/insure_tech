/* Load Raw .csv data from Snowflake tables, cast to proper field types. */
{{ config(materialized='view') }}

SELECT
  application_id::VARCHAR AS application_id,
  state::VARCHAR AS state,
  industry::VARCHAR AS industry,
  TRY_PARSE_JSON(sample_json) AS sample_json,
  TRY_TO_DATE(submission_date)::DATE AS submission_date,
  TRY_TO_DATE(updated_at)::DATE AS updated_at,
  __loaded_at::TIMESTAMP_NTZ AS __loaded_at,
  __file_name::VARCHAR AS __file_name,
  CURRENT_TIMESTAMP() AS __dbt_processed_at
FROM {{ source('insurance', 'submissions') }}
