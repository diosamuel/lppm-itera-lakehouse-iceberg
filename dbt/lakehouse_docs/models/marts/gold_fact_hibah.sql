{{ config(materialized='table') }}

select
    md5(cast(s.id as varchar) || cast(s.tahun as varchar)) as hibah_fact_id,
    md5(cast(s.id as varchar)) as hibah_id,
    md5(cast(s.nip_ketua_peneliti as varchar)) as ketua_id,
    s.usulan_biaya as usulan_biaya,
    s.tahun,
    s.status as status_hibah,
    current_timestamp as loaded_at
from {{ ref('silver_penelitian_cleaned') }} s
