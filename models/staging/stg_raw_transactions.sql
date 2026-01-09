with source as (

    select * from {{ source('fraud_raw', 'raw_transactions') }}

),

renamed as (

    select
        -- IDs: explicit STRING casts are crucial for BI tools (prevents summing IDs)
        cast(transaction_id as string) as transaction_id,
        cast(customer_id as string) as customer_id,
        cast(merchant_id as string) as merchant_id,

        -- Timestamps: explicit TIMESTAMP cast
        cast(timestamp as timestamp) as transaction_at,

        -- Financials: explicit NUMERIC cast (better than Float for money)
        cast(amount as numeric) as amount,

        -- Dimensions: explicit STRING cast
        cast(category as string) as category,
        
        -- Logic: explicit BOOLEAN cast
        cast(is_suspicious_flag as boolean) as is_suspicious_flag

    from source

)

select * from renamed