{{ config(
    materialized = 'table',
    properties   = {
        "engine"   : "'MergeTree'",
        "order_by" : "ARRAY['feature_date', 'customer_id']"
    }
) }}

-- feature_date is injected via Airflow --vars; defaults to yesterday for local runs
{% set feature_date = var('feature_date', 'current_date - interval \'1\' day') %}

SELECT
    customer_id,
    CAST({{ feature_date }} AS DATE)                                                AS feature_date,

    COUNT(CASE WHEN event_date = CAST({{ feature_date }} AS DATE) THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,

    COUNT(CASE WHEN event_date >= CAST({{ feature_date }} AS DATE) - INTERVAL '6' DAY THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,

    COUNT(CASE WHEN event_date >= CAST({{ feature_date }} AS DATE) - INTERVAL '29' DAY THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,

    COALESCE(
        AVG(CASE WHEN event_date = CAST({{ feature_date }} AS DATE) THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,

    COALESCE(
        AVG(CASE WHEN event_date >= CAST({{ feature_date }} AS DATE) - INTERVAL '6' DAY THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,

    COALESCE(
        AVG(CASE WHEN event_date >= CAST({{ feature_date }} AS DATE) - INTERVAL '29' DAY THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_30D

FROM {{ ref('stg_transactions') }}
WHERE event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '29' DAY
                     AND CAST({{ feature_date }} AS DATE)
GROUP BY customer_id
