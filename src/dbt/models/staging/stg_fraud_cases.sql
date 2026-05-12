{{ config(
    materialized         = 'incremental',
    unique_key           = 'case_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'fraud_cases') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_bronze_ingested_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY case_id
               ORDER BY _ingested_at DESC
           ) AS _rn
    FROM source
)

SELECT
    case_id,
    transaction_id,
    CAST(
        COALESCE(case_status = 'confirmed' AND resolved_at IS NOT NULL, FALSE)
        AS BOOLEAN
    ) AS is_fraud,
    _op                AS _cdc_op,
    _ingested_at       AS _bronze_ingested_at,
    CURRENT_TIMESTAMP  AS _staging_updated_at
FROM deduped
WHERE _rn = 1
