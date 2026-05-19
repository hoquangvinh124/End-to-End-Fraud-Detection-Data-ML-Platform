{{ config(
    materialized = 'incremental',
    unique_key   = 'transaction_id',
    incremental_strategy = 'append',
    views_enabled = false,
    properties   = {
        "engine": "'MergeTree'"
    }
) }}

{% set feature_date = var('feature_date', none) %}
{% if feature_date %}
    {% set fd_expr = "DATE '" ~ feature_date ~ "'" %}
{% else %}
    {% set fd_expr = "current_date - interval '1' day" %}
{% endif %}


WITH params AS (
    SELECT CAST({{ fd_expr }} AS DATE) AS fd
),


fraud_per_tx AS (
    SELECT
        transaction_id,
        IF(MAX(is_fraud), 1, 0)  AS tx_fraud_int
    FROM {{ ref('stg_fraud_cases') }}
    GROUP BY transaction_id
)


SELECT
    t.transaction_id,
    CAST(t.event_timestamp AS timestamp(0))                                          AS event_timestamp,


    -- Transaction features (direct from staging)
    CAST(t.amount    AS DOUBLE)                                              AS TX_AMOUNT,
    t.is_weekend                                                             AS IS_WEEKEND,
    t.is_night                                                               AS IS_NIGHT,


    -- Customer window features (LEFT JOIN: new customers default to 0.0)
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_1D              AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_7D              AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_30D             AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_30D,
    COALESCE(CAST(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D  AS DOUBLE), 0.0)  AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
    COALESCE(CAST(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D  AS DOUBLE), 0.0)  AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
    COALESCE(CAST(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D AS DOUBLE), 0.0)  AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,


    -- Terminal window features (LEFT JOIN: new terminals default to 0.0)
    COALESCE(CAST(tm.TERMINAL_RISK_1DAY_WINDOW   AS DOUBLE), 0.0)            AS TERMINAL_RISK_1DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_RISK_7DAY_WINDOW   AS DOUBLE), 0.0)            AS TERMINAL_RISK_7DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_RISK_30DAY_WINDOW  AS DOUBLE), 0.0)            AS TERMINAL_RISK_30DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_NB_TX_1DAY_WINDOW  AS DOUBLE), 0.0)            AS TERMINAL_NB_TX_1DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_NB_TX_7DAY_WINDOW  AS DOUBLE), 0.0)            AS TERMINAL_NB_TX_7DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_NB_TX_30DAY_WINDOW AS DOUBLE), 0.0)            AS TERMINAL_NB_TX_30DAY_WINDOW,


    -- Fraud label (deduped subquery: missing fraud_case → 0 = legitimate)
    COALESCE(f.tx_fraud_int, 0)                                              AS TX_FRAUD,


    p.fd                                                                     AS feature_date


FROM {{ ref('stg_transactions') }} t
CROSS JOIN params p
LEFT JOIN {{ ref('int_customers_windowed') }} c
    ON  t.customer_id  = c.customer_id
    AND c.feature_date = p.fd
LEFT JOIN {{ ref('int_terminals_windowed') }} tm
    ON  t.terminal_id  = tm.terminal_id
    AND tm.feature_date = p.fd
LEFT JOIN fraud_per_tx f
    ON t.transaction_id = f.transaction_id
WHERE t.event_date = p.fd