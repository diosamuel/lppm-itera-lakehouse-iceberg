{{ config(materialized="table") }}

select * from {{ ref("silver_penelitian") }}
union
select * from {{ ref("silver_pengabdian") }}
union
select * from {{ ref("silver_buku_keilmuan") }}