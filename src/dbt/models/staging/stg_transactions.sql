{{ config(
    materialized         = 'incremental',
    unique_key           = 'transaction_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'transactions') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_bronze_ingested_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY transaction_id
               ORDER BY _ingested_at DESC
           ) AS _rn
    FROM source
)

SELECT
    transaction_id,
    CAST(event_timestamp AS TIMESTAMP)           AS event_timestamp,
    CAST(DATE(event_timestamp) AS DATE)          AS event_date,
    TRY_CAST(customer_id AS BIGINT)              AS customer_id,
    TRY_CAST(terminal_id AS BIGINT)              AS terminal_id,
    CAST(amount AS DECIMAL(12,2))                AS amount,
    CAST(is_weekend AS BOOLEAN)                  AS is_weekend,
    CAST(is_night AS BOOLEAN)                    AS is_night,
    _op                                          AS _cdc_op,
    _ingested_at                                 AS _bronze_ingested_at,
    CURRENT_TIMESTAMP                            AS _staging_updated_at
FROM deduped
WHERE _rn = 1
