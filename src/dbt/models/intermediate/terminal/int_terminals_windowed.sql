{{ config(
    materialized         = 'incremental',
    unique_key           = ['terminal_id', 'feature_date'],
    incremental_strategy = 'merge',
    on_schema_change     = 'fail'
) }}

{% set feature_date = var('feature_date', none) %}
{% if feature_date %}
    {% set fd_expr = "DATE '" ~ feature_date ~ "'" %}
{% else %}
    {% set fd_expr = "current_date - interval '1' day" %}
{% endif %}

WITH labeled AS (
    SELECT
        t.terminal_id,
        t.event_date,
        COALESCE(f.is_fraud, false) AS is_fraud
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_fraud_cases') }} f
        ON t.transaction_id = f.transaction_id
    WHERE t.event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '36' DAY
                           AND CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
)

SELECT
    terminal_id,
    CAST({{ fd_expr }} AS DATE)                                                     AS feature_date,

    -- 1-day window (delay-offset: fd-7)
    COUNT(CASE WHEN event_date = CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_1DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date = CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date = CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_1DAY_WINDOW,

    -- 7-day window (delay-offset: [fd-13, fd-7])
    COUNT(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '13' DAY
                                   AND CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_7DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '13' DAY
                                         AND CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '13' DAY
                                              AND CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_7DAY_WINDOW,

    -- 30-day window (delay-offset: [fd-36, fd-7])
    COUNT(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '36' DAY
                                   AND CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_30DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '36' DAY
                                         AND CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '36' DAY
                                              AND CAST({{ fd_expr }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_30DAY_WINDOW

FROM labeled
GROUP BY terminal_id
