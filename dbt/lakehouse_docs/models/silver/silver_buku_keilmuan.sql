{{ config(materialized='table') }}

select * from {{ source('bronze', 'buku_keilmuan') }}
