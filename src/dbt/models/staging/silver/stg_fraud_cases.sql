with source as (

    select * from {{ source('silver', 'fraud_cases') }}

),

renamed as (

    select
        case_id,
        transaction_id,
        customer_id,
        card_id,
        fraud_scenario,
        case_status,
        resolution_source,
        reported_at,
        resolved_at,
        loss_amount,
        created_at,
        is_fraud,
        reported_date

    from source

)

select * from renamed