{{ config(
    materialized         = 'incremental',
    unique_key           = ['customer_id', 'feature_date'],
    incremental_strategy = 'merge',
    on_schema_change     = 'fail'
) }}

{% set feature_date = var('feature_date', none) %}
{% if feature_date %}
    {% set fd_expr = "DATE '" ~ feature_date ~ "'" %}
{% else %}
    {% set fd_expr = "current_date - interval '1' day" %}
{% endif %}

SELECT
    customer_id,
    CAST({{ fd_expr }} AS DATE)                                                     AS feature_date,

    COUNT(CASE WHEN event_date = CAST({{ fd_expr }} AS DATE) THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,

    COUNT(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '6' DAY
                                   AND CAST({{ fd_expr }} AS DATE) THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,

    COUNT(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '29' DAY
                                   AND CAST({{ fd_expr }} AS DATE) THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,

    COALESCE(
        AVG(CASE WHEN event_date = CAST({{ fd_expr }} AS DATE) THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,

    COALESCE(
        AVG(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '6' DAY
                                     AND CAST({{ fd_expr }} AS DATE) THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,

    COALESCE(
        AVG(CASE WHEN event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '29' DAY
                                     AND CAST({{ fd_expr }} AS DATE) THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_30D

FROM {{ ref('stg_transactions') }}
WHERE event_date BETWEEN CAST({{ fd_expr }} AS DATE) - INTERVAL '29' DAY
                     AND CAST({{ fd_expr }} AS DATE)
GROUP BY customer_id
