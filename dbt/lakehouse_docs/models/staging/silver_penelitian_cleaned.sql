{{ config(materialized='view') }}

with raw as (
    select * from {{ ref('bronze_penelitian') }}
)

select
    id,
    trim(judul_proposal) as judul_proposal,
    trim(ketua_peneliti) as ketua_peneliti,
    upper(jenis) as jenis,
    upper(status) as status,
    upper(trim(skema)) as skema,
    upper(trim(scope)) as scope,
    upper(trim(sdgs)) as sdgs,
    cast(regexp_replace(usulan_biaya, '[^0-9.]', '') as decimal(15,2)) as usulan_biaya,
    upper(status_proposal) as status_proposal,
    cast(tahun as int) as tahun,
    upper(trim(prodi)) as prodi,
    upper(trim(fakultas)) as fakultas,
    upper(trim(nip_ketua_peneliti)) as nip_ketua_peneliti,
    nim_anggota_mahasiswa,
    nama_anggota_mahasiswa,
    nip_anggota_dosen,
    nama_anggota_dosen,
    advisor
from raw
where tahun is not null
