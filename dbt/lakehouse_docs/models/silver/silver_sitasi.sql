{{ config(materialized='view') }}

select *
from {{ source('bronze', 'sitasi') }}
