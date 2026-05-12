{{ config(
    materialized = 'table',
    properties   = {
        "engine"   : "'MergeTree'",
        "order_by" : "ARRAY['feature_date', 'transaction_id']"
    }
) }}

{% set feature_date = var('feature_date', none) %}
{% if feature_date %}
    {% set fd_expr = "DATE '" ~ feature_date ~ "'" %}
{% else %}
    {% set fd_expr = "current_date - interval '1' day" %}
{% endif %}

SELECT
    t.transaction_id,
    t.event_timestamp,

    -- Transaction features (direct from staging)
    CAST(t.amount AS DOUBLE)                                          AS TX_AMOUNT,
    t.is_weekend                                                      AS IS_WEEKEND,
    t.is_night                                                        AS IS_NIGHT,

    -- Customer window features (LEFT JOIN: new customers default to 0)
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_1D  AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_7D  AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_30D AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_30D,
    COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,  0)        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
    COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,  0)        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
    COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D, 0)        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,

    -- Terminal window features (LEFT JOIN: new terminals default to 0)
    COALESCE(CAST(tm.TERMINAL_RISK_1DAY_WINDOW  AS DOUBLE), 0.0)     AS TERMINAL_RISK_1DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_RISK_7DAY_WINDOW  AS DOUBLE), 0.0)     AS TERMINAL_RISK_7DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_RISK_30DAY_WINDOW AS DOUBLE), 0.0)     AS TERMINAL_RISK_30DAY_WINDOW,
    COALESCE(tm.TERMINAL_NB_TX_1DAY_WINDOW,  0)                      AS TERMINAL_NB_TX_1DAY_WINDOW,
    COALESCE(tm.TERMINAL_NB_TX_7DAY_WINDOW,  0)                      AS TERMINAL_NB_TX_7DAY_WINDOW,
    COALESCE(tm.TERMINAL_NB_TX_30DAY_WINDOW, 0)                      AS TERMINAL_NB_TX_30DAY_WINDOW,

    -- Fraud label (LEFT JOIN: missing fraud_case → legitimate = 0)
    COALESCE(CAST(f.is_fraud AS INTEGER), 0)                         AS TX_FRAUD,

    CAST({{ fd_expr }} AS DATE)                                      AS feature_date

FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('int_customer_window_features') }} c
    ON t.customer_id = c.customer_id
LEFT JOIN {{ ref('int_terminal_window_features') }} tm
    ON t.terminal_id = tm.terminal_id
LEFT JOIN {{ ref('stg_fraud_cases') }} f
    ON t.transaction_id = f.transaction_id
WHERE t.event_date = CAST({{ fd_expr }} AS DATE)
