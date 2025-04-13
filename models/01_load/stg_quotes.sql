/* Load Raw .csv data from Snowflake tables, cast to proper field types. */
{{ config(materialized='view') }}

SELECT
  application_id::VARCHAR AS application_id,
  quote_id::VARCHAR AS quote_id,
  quoted_date_date::DATE AS quoted_date,
  unnamed_0_::INTEGER AS _file_row_number,
  __loaded_at::TIMESTAMP_NTZ AS __loaded_at,
  __file_name::VARCHAR AS __file_name,
  CURRENT_TIMESTAMP() AS __dbt_processed_at
FROM {{ source('insurance', 'quotes') }}