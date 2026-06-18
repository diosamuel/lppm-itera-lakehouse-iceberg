## Ringkasan
Alur kerja ini menyelesaikan duplikasi dan inkonsistensi nama dosen menggunakan pendekatan **Write–Audit–Publish (WAP)**. Tujuannya adalah memastikan berbagai variasi nama untuk satu orang yang sama dikelompokkan dan direpresentasikan oleh satu data saja.

Dalam kasus ini, seluruh variasi **"Virdio"** akan dipersatukan, dan nama real yang dipilih adalah:
> **Virdio S.T., M.T.**

---

# TL;DR
1. Write — Data mentah masuk, tiap nama dapat ID hash
2. Audit — Nama dibersihkan (gelar/punctuation dihapus), lalu di-cluster berdasarkan kemiripan
3. Keputusan — Tiap cluster pilih 1 nama real, sisanya ditandai duplikat
4. Publish — Filter hanya nama real → dataset bersih siap pakai

# 1. WRITE (Lapisan Data Mentah)

## MASUKAN
Dataset mentah yang berisi nama dosen:

- Virdio S.T., M.T.
- Virdio S.T. M.T
- Virdio
- Jokowi
- Budi Santoso

---

## PROSES
Setiap record diberi ID deterministik menggunakan hash dari nama mentah. Ini memastikan setiap baris dapat dilacak secara unik.

---

## KELUARAN
DataFrame yang berisi:
- `dosen` (nama mentah)
- `id` (identifier hasil hash dari nama mentah)

---

# 2. AUDIT (Lapisan Pembersihan + Pengelompokan)

## MASUKAN
DataFrame dengan:
- nama mentah (`dosen`)
- ID mentah (`id`)

---

## PROSES

### 1. Pembersihan Nama
Semua nama dosen dinormalisasi dengan cara:
- menghapus gelar akademik (S.T., M.T., dll.)
- menghapus tanda baca
- menyeragamkan huruf

Hasil nama yang sudah dibersihkan:

- "Virdio S.T., M.T." → "virdio"
- "Virdio S.T. M.T" → "virdio"
- "Virdio" → "virdio"
- "Jokowi" → "jokowi"
- "Budi Santoso" → "budi santoso"

---

### 2. Pengelompokan (Clustering)
Record dikelompokkan berdasarkan kemiripan nama yang sudah dibersihkan.

---

## KELUARAN (Cluster)

- Cluster 1:
  - ("virdio", id1)
  - ("virdio", id2)
  - ("virdio", id3)

- Cluster 2:
  - ("jokowi", id4)

- Cluster 3:
  - ("budi santoso", id5)

---

# 3. KEPUTUSAN (Pemilihan Nama Real)

## MASUKAN
Kelompok cluster dari tahap audit.

---

## PROSES
Untuk setiap cluster, satu record dipilih sebagai entitas Nama Real.

Untuk cluster "virdio":
- Record Nama Real yang dipilih adalah:
  > **Virdio S.T., M.T.**

Variasi lain ditandai sebagai duplikat.

---

## KELUARAN
Dataset berlabel dengan:

- `is_chosen = True` → record Nama Real
- `is_chosen = False` → record duplikat

Contoh (cluster Virdio):

- Virdio S.T., M.T. → TRUE
- Virdio S.T. M.T → FALSE
- Virdio → FALSE

---

# 4. PUBLISH (Dataset Bersih Akhir)

## MASUKAN
Dataset lengkap dengan flag `is_chosen`.

---

## PROSES
Filter hanya record dengan:
- `is_chosen = True`

---

## KELUARAN (Dataset Akhir)

Tabel dosen bersih dan sudah deduplikasi:

- Virdio S.T., M.T.
- Jokowi
- Budi Santoso

---

# 5. HASIL AKHIR

Sistem berhasil:
- menggabungkan variasi nama dosen yang duplikat
- mempertahankan jejak audit menggunakan ID mentah
- memilih representasi Nama Real untuk setiap entitas
- menghasilkan dataset bersih yang siap digunakan untuk lapisan gold / kebutuhan BI

---