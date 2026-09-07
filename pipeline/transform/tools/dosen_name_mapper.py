"""Mapper nama dosen silver.sitasi -> gold.dim_dosen.

DOSEN_NAME_MAPPING maps a silver.sitasi ketua_peneliti (Google Scholar name
variant) to its gold.dim_dosen nama counterpart. Names not present in the
mapping are returned unchanged: they either already match dim_dosen exactly
or are known-unresolvable (author lists, publishers, dosen without hibah
records).

Usage in pipeline/index.py:

    from tools.dosen_name_mapper import map_dosen_name_udf

    res = res.withColumn("ketua_peneliti", map_dosen_name_udf(F.col("ketua_peneliti")))
"""

from tools.nama_dosen_audit import standardize_nama_dosen
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

DOSEN_NAME_MAPPING = {
    "Achmad Samudra Dewantara.": "Achmad Samudra Dewantara, S.t., M.t.",
    "Aditya Ayuwulanda, M.si.": "Aditya Ayuwulanda, S.pd., M.si.",
    "Aditya Rianjanu.": "Dr. Aditya Rianjanu, S.si.",
    "Aditya Wahyu Nugraha.": "Dr. Aditya Wahyu Nugraha, S.tp., M.si.",
    "Aldila Nur Indah Berliana Ratam.": "Aldila Nur Indah Berliana Ratam, S.si. M.si.",
    "Alvin Pratama.": "Alvin Pratama, S.si., M.t.",
    "Andhyka Tyaz Nugraha.": "Andhyka Tyaz Nugraha, S.p., M.si., Ph.d.",
    "Andika Setiawan.": "Andika Setiawan, S.kom., M.cs.",
    "Andre Febrianto.": "Andre Febrianto, S.kom., M.eng.",
    "Apt. Novrilia Atika Nabila, S.farm., M.clin.pharm.": "Apt. Novrilia Atika Nabila, M.clin. Pharm.",
    "Ardika Satria, M.si.": "Ardika Satria, S.si., M.si.",
    "Arif Ashari.": "Arif Ashari, S.si., M.si.",
    "Aulia Annas Mufti.": "Aulia Annas Mufti, S.t., M.eng.",
    "Bayu Prasetya.": "Bayu Prasetya, S.si., M.t.",
    "Bilal Al Farishi.": "Bilal Al Farishi, B.sc(hons)., M.sc.",
    "Burhaan Shodiq.": "Burhaan Shodiq, M.or.",
    "Damayanti.": "Damayanti, S.t., M.sc.",
    "David Septian Sumanto Marpaung.": "David Septian Sumanto Marpaung, S.t.p., M.sc.",
    "Deska Lismawenning Puspitarum.": "Dr. Deska Lismawenning Puspitarum, S.si., M.sc.",
    "Dr. Apt. Anjar Hermadi Saputro, S.farm., M.pharm.sci.": "Dr. Apt. Anjar Hermadi Saputro, S.farm., M.pharm.s.",
    "Eka Nurfani.": "Dr. Eka Nurfani, S.si., M.si.",
    "Elian Zhafira.": "Elian Zhafira, S.t., M.t.",
    "Fajar Paundra.": "Fajar Paundra, S.t., M.t.",
    "Fuji Lestari.": "Fuji Lestari, M.si.",
    "Gres Maretta.": "Gres Maretta, S.si., M.si.",
    "Hadi Teguh Yudistira.": "Hadi Teguh Yudistira, S.t., Ph.d.",
    "Harlina Ardiyanti, M.sc.": "Harlina Ardiyanti, S.si., M.sc.",
    "Harry Yuliansyah.": "Harry Yuliansyah, S.t., M.eng.",
    "Iffa Afiqa Khairani.": "Iffa Afiqa Khairani, M.si.",
    "Intan Mardiono.": "Intan Mardiono, S.t., M.sc.",
    "Isye Susana Nurhasanah.": "Ir. Isye Susana Nurhasanah, S.t., M.si.(han), Ph.d.",
    "Jabosar Ronggur Hamonangan Panjaitan.": "Dr. Jabosar Ronggur Hamonangan Panjaitan, S.t., M.t.",
    "Jatmiko Eko Witoyo.": "Dr. Jatmiko Eko Witoyo, S.t.p., M.p.",
    "Khansa Salsabila Suhaimi.": "Khansa Salsabila Suhaimi, S.t., M.t.",
    "Leslie Anggraini, S.kom., M. Cs.": "Leslie Anggraini, S.kom., M.cs.",
    "M Gilang Indra Mardika.": "M Gilang Indra Mardika, S.t., M.t.",
    "Mahardika Yoga Darmawan.": "Mahardika Yoga Darmawan, S.t., M.sc.",
    "Maqbul, S.t. M.sc.": "Maqbul Kamaruddin, S.t., M.sc.",
    "Maria Yosephine Simbolon.": "Maria Yosephine Simbolon, S.kel., M.t.",
    "Mariyam.": "Dr. Mariyam, S.pd., M.sc.",
    "Martin C. T. Manullang.": "Martin Clinton Tosima Manullang, S.t., M.t., Ph.d.",
    "Mira Mustika.": "Mira Mustika, S.si., M.sc.",
    "Misbahudin Alhanif.": "Misbahudin Alhanif, S.t., M.t.",
    "Miskar Maini.": "Dr. Miskar Maini, S.t., M.eng.",
    "Mochammad Fathurridho Hermanto.": "Mochammad Fathurridho Hermanto, S.t., M.t.",
    "Mohamad Idris.": "Mohamad Idris, S.si., M.sc.",
    "Mugi Praseptiawan.": "Mugi Praseptiawan, S.t., M.kom.",
    "Muhammad Abdul Mubdi Bindar.": "Muhammad Abdul Mubdi Bindar, S.t., M.t.",
    "Muhammad Arhan Rajab.": "Muhammad Arhan Rajab, S.pi., M.si.",
    "Muhammad Asril.": "Dr. Muhammad Asril, S.si., M.si.",
    "Muhammad Hakiem Sedo Putra.": "Muhammad Hakiem Sedo Putra, S.t., M.t.",
    "Muhammad Yasin Siregar.": "Mhd. Yasin Siregar, M.t.",
    "Muhammad Yogi Saputra.": "Muhammad Yogi Saputra, S.si., M.si.",
    "Muhammad Zainal Ibad.": "Dr. Muhammad Zainal Ibad, S.t., M.t.",
    "Nabila Putriyandri Alifa.": "Nabila Putriyandri Alifa, S.t., M.sc.",
    "Nirmawana Simarmata.": "Dr. Nirmawana Simarmata, S.pd., M.sc.",
    "Novita Hillary Christy Damanik.": "Novita Hillary Christy Damanik, S.t., M.ars.",
    "Novrilia Nabila.": "Apt. Novrilia Atika Nabila, M.clin. Pharm.",
    "Nur Istiqomah Khamidy.": "Dr. Nur Istiqomah Khamidy, S.t., M.sc.",
    "Nurbaiti.": "Nurbaiti, M.t.p.",
    "Okti Ruenda.": "Okti Ruenda, S.t.p., M.t.p.",
    "Prio Santoso.": "Prio Santoso, S.si., M.sc.",
    "Putra Andi Kolala.": "Putra Andi Kolala, S.t., M.t.",
    "Putri Amelia Rooswita.": "Apt. Putri Amelia Rooswita, S.farm., M.farm.",
    "Putty Yunesti, M.eng.": "Putty Yunesti, S.t., M.eng.",
    "Rahmat Kurniawan.": "Dr. Rahmat Kurniawan, S.si., M.si.",
    "Rifka Noor Azizah.": "Rifka Noor Azizah I. S.t., M.t.",
    "Rifky Fauzi.": "Dr. Rifky Fauzi, S.si., M.si.",
    "Rio Ardiansyah Murda.": "Rio Ardiansyah Murda, S.hut., M.si.",
    "Riri Fauziyya.": "Apt. Riri Fauziyya, S.farm., M.farm.",
    "Rizki Kurnia Tohir.": "Rizki Kurnia Tohir, S.hut., M.si.",
    "Rizqi Wahyudi.": "Rizqi Wahyudi, S.tp., M.sc.",
    "Sarwono Sutikno.": "Prof. Dr. Eng. Ir. Sarwono Sutikno, Cisa.",
    "Satrio Muhammad Alif.": "Satrio Muhammad Alif, S.t., M.t.",
    "Sherin Ramadhania.": "Sherin Ramadhania, M.t.",
    "Siska Wulandari.": "Siska Wulandari, S.t., M.t.",
    "Suratun Nafisah.": "Dr. Suratun Nafisah, S.si. M.sc.",
    "Tarmizi Taher.": "Dr. Tarmizi Taher, S.si.",
    "Tri Siswandi Syahputra.": "Tri Siswandi Syahputra, S.si, M.si.",
    "Ulfah Izdihar.": "Ulfah Izdihar, S.t.p., M.sc.",
    "Wahyu Solafide Sipahutar.": "Wahyu Solafide Sipahutar, S.si., M.sc.",
    "Wenny Arminda.": "Wenny Arminda, S.t., M.sc., Ph.d.",
    "Werry Febrianti.": "Dr. Werry Febrianti, S.pd., M.si.",
    "Wika Atro Auriyani.": "Wika Atro Auriyani, S.t., M.t.",
    "Winda Yulita.": "Winda Yulita, S.pd., M.cs.",
    "Yudha Gusti Wibowo.": "Yudha Gusti Wibowo, S.t., M.si.",
    "Yuni Lisafitri.": "Yuni Lisafitri, S.p., M.si.",
    "Zulfikar Adlan Nadzir.": "Zulfikar Adlan Nadzir, S.t., M.sc.",
}

# Normalized lookup (degree/prefix-stripped) for robustness against
# formatting drift between silver exports.
_NORMALIZED_MAPPING = {}
for _silver, _dim in DOSEN_NAME_MAPPING.items():
    _std = standardize_nama_dosen(_silver)
    if _std:
        _NORMALIZED_MAPPING[_std] = _dim


def map_dosen_name(name):
    """Map a silver dosen name to its dim_dosen nama, or return it unchanged."""
    if name is None:
        return None
    name = str(name).strip()
    if not name:
        return name
    if name in DOSEN_NAME_MAPPING:
        return DOSEN_NAME_MAPPING[name]
    std = standardize_nama_dosen(name)
    if std and std in _NORMALIZED_MAPPING:
        return _NORMALIZED_MAPPING[std]
    return name


map_dosen_name_udf = F.udf(map_dosen_name, StringType())
