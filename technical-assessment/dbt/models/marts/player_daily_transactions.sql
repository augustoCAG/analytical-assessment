-- models/marts/player_daily_transactions.sql

with transactions as (

    select * from {{ ref('stg_transactions') }}

)

select
    player_id,
    date(transaction_at) as transaction_date,
    sum(case when transaction_type = 'Deposit' then amount else 0 end) as total_deposits,
    sum(case when transaction_type = 'Withdraw' then -amount else 0 end) as total_withdrawals
from transactions
group by 1, 2
order by 1, 2
