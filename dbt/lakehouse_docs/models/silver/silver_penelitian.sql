{{ config(materialized='view') }}

select *
from {{ source('bronze', 'penelitian') }}
