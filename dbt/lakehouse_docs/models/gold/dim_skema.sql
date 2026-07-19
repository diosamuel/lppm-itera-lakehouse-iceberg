{{ config(materialized='view') }}

select *
from {{ ref('hibah_lengkap') }}
