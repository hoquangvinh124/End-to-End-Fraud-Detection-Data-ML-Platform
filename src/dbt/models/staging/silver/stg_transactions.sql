with source as (

    select * from {{ source('silver', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        event_timestamp,
        customer_id,
        account_id,
        card_id,
        terminal_id,
        amount,
        currency_code,
        transaction_type,
        channel_type,
        auth_status,
        tx_time_seconds,
        tx_time_days,
        is_weekend,
        is_night,
        created_at,
        event_date

    from source

)

select * from renamed