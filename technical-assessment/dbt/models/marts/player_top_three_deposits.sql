-- models/marts/player_top_three_deposits.sql

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

ranked_deposits as (

    select
        player_id,
        amount,
        -- row_number() instead of rank() to handle cases where a player has multiple deposits of the same amount
        row_number() over (partition by player_id order by amount desc) as deposit_rank
    from transactions
    where transaction_type = 'Deposit'

)

select
    player_id,
    max(case when deposit_rank = 1 then amount end) as first_largest_deposit,
    max(case when deposit_rank = 2 then amount end) as second_largest_deposit,
    max(case when deposit_rank = 3 then amount end) as third_largest_deposit
from ranked_deposits
where deposit_rank <= 3
group by 1
order by 1
