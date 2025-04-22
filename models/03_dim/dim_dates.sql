/* Creates a date table which allows accurately reporting running hit rate values
, even on days where a quote or policy was not created */

{{ config(
    materialized='table',
    tags=['calendar', 'date_spine']
) }}

WITH all_dates AS (
    SELECT
        MIN(event_date) AS min_date,
        MAX(event_date) AS max_date
    FROM (
        SELECT submission_date AS event_date FROM {{ ref('fct_submissions') }}
        UNION ALL
        SELECT quoted_date FROM {{ ref('fct_quotes') }}
        UNION ALL
        SELECT policy_created_at FROM {{ ref('stg_policies') }}
    )
),

date_spine_raw AS (
    SELECT
        DATEADD(day, SEQ4(), (SELECT min_date FROM all_dates)) AS date,
        (SELECT max_date FROM all_dates) AS max_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1000))  -- set high enough for your date range
),

date_spine AS (
    SELECT date
    FROM date_spine_raw
    WHERE date <= max_date
)

SELECT
    date,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    DATE_TRUNC('week', date) AS week_start,
    DATE_TRUNC('month', date) AS month_start,
    DATE_TRUNC('quarter', date) AS quarter_start,
    DATE_TRUNC('year', date) AS year_start
FROM date_spine
ORDER BY date
