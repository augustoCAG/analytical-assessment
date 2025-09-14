-- models/staging/stg_players.sql

with source as (

    select * from {{ source('raw', 'players') }}

)

select
    id as player_id,
    affiliate_id,
    country_code,
    is_kyc_approved
from source
