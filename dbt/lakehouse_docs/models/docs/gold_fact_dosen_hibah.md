{% docs gold_fact_dosen_hibah %}

# Gold Layer: Table fakta dosen dengan hibah

Tabel fakta ini digunakan untuk menganalisis keterlibatan setiap dosen dalam kegiatan hibah berdasarkan peran atau posisi keanggotaannya.
Grain detail atau tingkat granularitas dari tabel ini adalah 1 baris merepresentasikan 1 dosen yang terlibat dalam 1 hibah tertentu.

contoh isi dari tabel fakta
| id | dosen_id | dosen | hibah_proposal_id | tahun | role | jenis_hibah | status_hibah |
|---|---|---|---|---|---|---|---|
| 1 | 96 | Jane Smith | BUKU_KEILMUAN-1 | 2023 | ketua | buku_keilmuan | ditolak |
| 2 | 113 | Michael Brown | BUKU_KEILMUAN-1 | 2023 | anggota | buku_keilmuan | ditolak |
| 3 | 790 | Emily Johnson | BUKU_KEILMUAN-10 | 2023 | ketua | buku_keilmuan | diterima |
| 4 | 209 | David Wilson | BUKU_KEILMUAN-11 | 2024 | anggota | buku_keilmuan | diterima |


{% enddocs %}
