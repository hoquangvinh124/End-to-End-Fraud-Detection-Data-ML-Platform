{% set today      = modules.datetime.date.today().isoformat() %}
{% set start_date = var('start_date', today) %}
{% set end_date   = var('end_date',   start_date) %}


SELECT
    src.terminal_id,
    d.fd                                                                         AS feature_date,

    -- 1-day window (delay-offset: fd-7)
    COUNT(CASE WHEN src.event_date = d.fd - INTERVAL '7' DAY THEN 1 END)
        AS TERMINAL_NB_TX_1DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN src.event_date = d.fd - INTERVAL '7' DAY AND src.is_fraud THEN 1.0 END)
            / NULLIF(COUNT(CASE WHEN src.event_date = d.fd - INTERVAL '7' DAY THEN 1 END), 0)
        AS DOUBLE),
        0.0
    ) AS TERMINAL_RISK_1DAY_WINDOW,

    -- 7-day window (delay-offset: [fd-13, fd-7])
    COUNT(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '13' DAY AND d.fd - INTERVAL '7' DAY THEN 1 END)
        AS TERMINAL_NB_TX_7DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '13' DAY AND d.fd - INTERVAL '7' DAY AND src.is_fraud THEN 1.0 END)
            / NULLIF(COUNT(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '13' DAY AND d.fd - INTERVAL '7' DAY THEN 1 END), 0)
        AS DOUBLE),
        0.0
    ) AS TERMINAL_RISK_7DAY_WINDOW,

    -- 30-day window (delay-offset: [fd-36, fd-7])
    COUNT(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '36' DAY AND d.fd - INTERVAL '7' DAY THEN 1 END)
        AS TERMINAL_NB_TX_30DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '36' DAY AND d.fd - INTERVAL '7' DAY AND src.is_fraud THEN 1.0 END)
            / NULLIF(COUNT(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '36' DAY AND d.fd - INTERVAL '7' DAY THEN 1 END), 0)
        AS DOUBLE),
        0.0
    ) AS TERMINAL_RISK_30DAY_WINDOW


FROM (
    SELECT t.terminal_id, t.event_date, COALESCE(f.is_fraud, false) AS is_fraud
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_fraud_cases') }} f ON t.transaction_id = f.transaction_id
    WHERE t.event_date BETWEEN DATE '{{ start_date }}' - INTERVAL '36' DAY AND DATE '{{ end_date }}'
) src
CROSS JOIN (
    SELECT CAST(d AS DATE) AS fd
    FROM UNNEST(SEQUENCE(DATE '{{ start_date }}', DATE '{{ end_date }}', INTERVAL '1' DAY)) AS t(d)
) d
WHERE src.event_date BETWEEN d.fd - INTERVAL '36' DAY AND d.fd - INTERVAL '7' DAY
GROUP BY src.terminal_id, d.fd
