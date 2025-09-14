-- models/staging/stg_transactions.sql

with source as (

    select * from {{ source('raw', 'transactions') }}

)

select
    id as transaction_id,
    "timestamp" as transaction_at,
    player_id,
    "type" as transaction_type,
    amount
from source
