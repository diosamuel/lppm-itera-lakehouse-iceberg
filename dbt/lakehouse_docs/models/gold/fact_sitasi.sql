{{ config(materialized='view') }}

select *
from {{ ref('silver_sitasi') }}
