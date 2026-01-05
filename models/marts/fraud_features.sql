with transactions as (

    select * from {{ ref('stg_raw_transactions') }}

),

-- LOGIC: Calculate "Features" that might indicate fraud
features as (

    select
        transaction_id,
        customer_id,
        transaction_at,
        amount,
        category,
        is_suspicious_flag, -- We keep this only to check our work later

        -- Feature 1: Velocity (How many transactions has this user made in the last hour?)
        -- "Senior Skill": Using Window Functions (COUNT OVER PARTITION)
        count(*) over (
            partition by customer_id 
            order by unix_seconds(transaction_at) 
            range between 3600 preceding and current row
        ) as velocity_last_hour,

        -- Feature 2: High Value (Is this transaction significantly larger than their average?)
        avg(amount) over (
            partition by customer_id
            order by unix_seconds(transaction_at)
            range between 2592000 preceding and current row -- Last 30 days
        ) as avg_spend_30_days

    from transactions

),

final as (

    select
        -- Retain IDs and timestamps from the previous step
        transaction_id,
        customer_id,
        transaction_at,
        amount,
        category,
        is_suspicious_flag,

        -- Explicitly cast the NEW features we calculated
        -- INT64 = Integer (Whole numbers for counts)
        cast(velocity_last_hour as INT64) as velocity_last_hour,

        -- NUMERIC = Money/Decimals
        cast(avg_spend_30_days as NUMERIC) as avg_spend_30_days,

        -- BOOL = True/False flag
        cast(
            case 
                when amount > (avg_spend_30_days * 5) then true 
                else false 
            end 
        as boolean) as is_high_amount_spike

    from features

)

select * from final