{{ config(
    materialized         = 'incremental',
    unique_key           = 'transaction_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail'
) }}

SELECT
    transaction_id,
    CAST(
        (case_status = 'confirmed' AND resolved_at IS NOT NULL)
        AS BOOLEAN
    )                                             AS is_fraud,
    _op                                           AS _cdc_op,
    _ingested_at                                  AS _bronze_ingested_at,
    CURRENT_TIMESTAMP                             AS _staging_updated_at
FROM {{ source('bronze', 'fraud_cases') }}
{% if is_incremental() %}
WHERE _ingested_at > (SELECT MAX(_staging_updated_at) FROM {{ this }})
{% endif %}
