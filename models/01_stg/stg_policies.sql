/* 1. Load Raw .csv data from Snowflake tables
   2. Cast to proper field types.
*/

{{ config(materialized='view') }}

SELECT
  policy_id::VARCHAR AS policy_id,
  quote_id::VARCHAR AS quote_id,
  policy_created_at_date::DATE AS policy_created_at,
  unnamed_0_::INTEGER AS _file_row_number,
  __loaded_at::TIMESTAMP_NTZ AS __loaded_at,
  __file_name::VARCHAR AS __file_name,
  CURRENT_TIMESTAMP() AS __dbt_processed_at
FROM {{ source('insurance', 'policies') }}