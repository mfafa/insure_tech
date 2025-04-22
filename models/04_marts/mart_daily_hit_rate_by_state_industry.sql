{{ config(
    materialized='table',
    tags=['mart', 'hit_rate', 'insurance']
) }}

WITH lifecycle AS (
    SELECT *
    FROM {{ ref('fct_insurance_lifecycle_events') }}
),

daily_counts AS (
    SELECT
        event_date,
        state,
        industry,
        SUM(quote_count) AS daily_quote_count,
        SUM(policy_count) AS daily_policy_count
    FROM lifecycle
    GROUP BY event_date, state, industry
),

with_running_totals AS (
    SELECT
        event_date,
        state,
        industry,
        daily_quote_count,
        daily_policy_count,

        SUM(daily_quote_count) OVER (
            PARTITION BY state, industry
            ORDER BY event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_quote_count,

        SUM(daily_policy_count) OVER (
            PARTITION BY state, industry
            ORDER BY event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_policy_count
    FROM daily_counts
)

SELECT
    event_date,
    state,
    industry,
    daily_quote_count,
    daily_policy_count,
    running_quote_count,
    running_policy_count,
    CASE 
        WHEN running_quote_count = 0 THEN NULL
        ELSE ROUND(running_policy_count * 1.0 / running_quote_count, 4)
    END AS running_hit_rate
FROM with_running_totals
ORDER BY event_date, state, industry
