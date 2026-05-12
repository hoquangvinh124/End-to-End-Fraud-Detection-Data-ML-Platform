{{ config(
    materialized = 'table',
    properties   = {
        "engine"   : "'MergeTree'",
        "order_by" : "ARRAY['feature_date', 'terminal_id']"
    }
) }}

{% set feature_date = var('feature_date', 'current_date - interval \'1\' day') %}

-- Attach fraud label; missing fraud_case rows → is_fraud = false (legitimate)
WITH labeled AS (
    SELECT
        t.terminal_id,
        t.event_date,
        COALESCE(f.is_fraud, false) AS is_fraud
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_fraud_cases') }} f
        ON t.transaction_id = f.transaction_id
    WHERE t.event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '37' DAY
                           AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
)

SELECT
    terminal_id,
    CAST({{ feature_date }} AS DATE)                                                AS feature_date,

    -- 1-day window (delay-offset: [fd-8, fd-7])
    COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '8' DAY
                                   AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_1DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '8' DAY
                                         AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '8' DAY
                                              AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_1DAY_WINDOW,

    -- 7-day window (delay-offset: [fd-14, fd-7])
    COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '14' DAY
                                   AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_7DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '14' DAY
                                         AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '14' DAY
                                              AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_7DAY_WINDOW,

    -- 30-day window (delay-offset: [fd-37, fd-7])
    COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '37' DAY
                                   AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_30DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '37' DAY
                                         AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '37' DAY
                                              AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_30DAY_WINDOW

FROM labeled
GROUP BY terminal_id
