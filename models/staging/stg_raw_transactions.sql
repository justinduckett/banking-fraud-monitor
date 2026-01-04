with source as (

    -- logic: grab the raw data from the source we just defined in the YML file
    select * from {{ source('fraud_raw', 'raw_transactions') }}

),

renamed as (

    select
        -- IDs
        transaction_id,
        customer_id,
        merchant_id,

        -- Timestamps: Explicitly cast to ensure BigQuery treats it as time, not text
        cast(timestamp as timestamp) as transaction_at,

        -- Numerics: Ensure money is treated as numbers
        cast(amount as numeric) as amount,

        -- Dimensions
        category,
        
        -- Logic: We keep the flag for testing, but in real life you might rename it
        cast(is_suspicious_flag as boolean) as is_suspicious_flag

    from source

)

select * from renamed