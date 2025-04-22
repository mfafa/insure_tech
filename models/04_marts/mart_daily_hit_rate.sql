{{ config(
    materialized='table',
    tags=['mart', 'hit_rate', 'insurance']
) }}

WITH base AS (
    SELECT
        event_date,
        quote_count,
        policy_count
    FROM {{ ref('fct_insurance_lifecycle_events') }}
    WHERE event_date IS NOT NULL
),

-- Step 1: Aggregate by day
daily_counts AS (
    SELECT
        event_date,
        SUM(quote_count) AS daily_quote_count,
        SUM(policy_count) AS daily_policy_count
    FROM base
    GROUP BY event_date
),

-- Step 2: Add running totals
with_running_totals AS (
    SELECT
        event_date,
        daily_quote_count,
        daily_policy_count,

        SUM(daily_quote_count) OVER (
            ORDER BY event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_quote_count,

        SUM(daily_policy_count) OVER (
            ORDER BY event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_policy_count
    FROM daily_counts
)

-- Final select
SELECT
    event_date,
    daily_quote_count,
    daily_policy_count,
    running_quote_count,
    running_policy_count,
    CASE 
        WHEN running_quote_count = 0 THEN NULL
        ELSE ROUND(running_policy_count * 1.0 / running_quote_count, 4)
    END AS running_hit_rate
FROM with_running_totals
ORDER BY event_date
