with transactions as (

    select * from {{ ref('stg_raw_transactions') }}

),

-- Calculate behavioral features that feed the fraud detection rules
features as (

    select
        transaction_id,
        customer_id,
        transaction_at,
        amount,
        category,
        is_suspicious_flag, -- Ground truth label from the simulator. Kept for evaluating rule performance, never used as an input.

        -- Feature 1: Velocity. How many transactions has this customer made
        -- in the last hour, INCLUDING this one? The current row is included
        -- deliberately: the transaction being scored is part of the burst
        -- it belongs to.
        count(*) over (
            partition by customer_id
            order by unix_seconds(transaction_at)
            range between 3600 preceding and current row
        ) as velocity_last_hour,

        -- Feature 2: Spending baseline. Average amount over the prior 30 days,
        -- EXCLUDING this transaction (note: 1 preceding, not current row).
        -- The baseline must not include the event being scored, otherwise a
        -- large fraud inflates the very average it is compared against.
        avg(amount) over (
            partition by customer_id
            order by unix_seconds(transaction_at)
            range between 2592000 preceding and 1 preceding
        ) as avg_spend_30_days

    from transactions

),

final as (

    select
        transaction_id,
        customer_id,
        transaction_at,
        amount,
        category,
        is_suspicious_flag,

        -- Explicit casts: INT64 for counts, NUMERIC for money
        cast(velocity_last_hour as INT64) as velocity_last_hour,
        cast(avg_spend_30_days as NUMERIC) as avg_spend_30_days,

        -- Rule 1: High amount spike. Flag transactions over 5x the customer's
        -- 30-day baseline. A customer's first transaction has no baseline
        -- (null average) and is never flagged: we cannot judge a deviation
        -- without history. This policy is documented in _marts.yml.
        cast(
            case
                when avg_spend_30_days is null then false
                when amount > avg_spend_30_days * 5 then true
                else false
            end
        as boolean) as is_high_amount_spike,

        -- Rule 2: Velocity attack. More than 3 transactions in a rolling hour.
        -- Defined here rather than in the BI tool so the threshold is
        -- version controlled, tested, and consistent everywhere.
        cast(velocity_last_hour > 3 as boolean) as is_velocity_attack

    from features

)

select * from final