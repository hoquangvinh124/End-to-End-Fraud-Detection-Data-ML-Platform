{{ config(
    materialized         = 'incremental',
    unique_key           = 'case_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'fail'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('lakehouse', 'fraud_cases') }}
    {% if is_incremental() %}
    WHERE _silver_updated_at > (SELECT MAX(_silver_updated_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY case_id
               ORDER BY _silver_updated_at DESC
           ) AS _rn
    FROM source
)

SELECT
    case_id,
    transaction_id,
    is_fraud,
    _cdc_op,
    _silver_updated_at,
    CURRENT_TIMESTAMP  AS _staging_updated_at
FROM deduped
WHERE _rn = 1
