-- models/marts/country_deposit_analysis.sql

with players as (
    select * from {{ ref('stg_players') }}
),

affiliates as (
    select * from {{ ref('stg_affiliates') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

player_affiliates as (
    select
        pla.player_id,
        pla.country_code,
    from players p
    left join affiliates aff
        on pla.affiliate_id = aff.affiliate_id
    where
        pla.is_kyc_approved = true
        and aff.affiliate_origin = 'Discord'
)

select
    paf.country_code,
    sum(tra.amount) as total_deposit_amount,
    count(tra.transaction_id) as number_of_deposits
from player_affiliates paf
join transactions tra
    on paf.player_id = tra.player_id
where tra.transaction_type = 'Deposit'
group by 1
order by 1
