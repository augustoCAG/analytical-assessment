-- models/staging/stg_affiliates.sql

with source as (

    select * from {{ source('raw', 'affiliates') }}

)

select
    id as affiliate_id,
    "origin" as affiliate_origin
from source
