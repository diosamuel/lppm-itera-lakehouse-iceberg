{{ config(materialized='view') }}

select *
from {{ ref('lookup_skema') }}
