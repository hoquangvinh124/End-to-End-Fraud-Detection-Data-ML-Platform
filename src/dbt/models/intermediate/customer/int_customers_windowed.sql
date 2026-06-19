{% set today      = modules.datetime.date.today().isoformat() %}
{% set start_date = var('start_date', today) %}
{% set end_date   = var('end_date',   start_date) %}


SELECT
    t.customer_id,
    d.fd                                                                         AS feature_date,

    COUNT(CASE WHEN t.event_date = d.fd THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,

    COUNT(CASE WHEN t.event_date BETWEEN d.fd - INTERVAL '6' DAY AND d.fd THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,

    COUNT(CASE WHEN t.event_date BETWEEN d.fd - INTERVAL '29' DAY AND d.fd THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,

    COALESCE(AVG(CASE WHEN t.event_date = d.fd THEN t.amount END), 0.0)
        AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,

    COALESCE(AVG(CASE WHEN t.event_date BETWEEN d.fd - INTERVAL '6' DAY AND d.fd THEN t.amount END), 0.0)
        AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,

    COALESCE(AVG(CASE WHEN t.event_date BETWEEN d.fd - INTERVAL '29' DAY AND d.fd THEN t.amount END), 0.0)
        AS CUSTOMER_AVG_AMOUNT_WINDOW_30D


FROM {{ ref('stg_transactions') }} t
CROSS JOIN (
    SELECT CAST(d AS DATE) AS fd
    FROM UNNEST(SEQUENCE(DATE '{{ start_date }}', DATE '{{ end_date }}', INTERVAL '1' DAY)) AS t(d)
) d
WHERE t.event_date BETWEEN DATE '{{ start_date }}' - INTERVAL '29' DAY AND DATE '{{ end_date }}'
GROUP BY t.customer_id, d.fd
