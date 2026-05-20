import re

"""
matchUniqueID: Nomor Induk Pegawai
"""
def matchUniqueID(text):
    if isinstance(text, list):
        result=list(map(lambda x: re.findall(r'\((.*?)\)', x),text))
        return result

    elif isinstance(text, str):
        return re.findall(r'\((.*?)\)', text)

    else:
        return None

"""
getFaculty: singkatan ke nama lengkap
"""
def getFaculty(text):
    mapper: dict = {
        "FS": "Fakultas Sains",
        "FTI": "Fakultas Teknologi Industri",
        "FTIK": "Fakultas Teknologi Industri dan Kewilayahan"
    }
    return mapper[text.split("-")[0].strip()]

"""
mapFacultyDegree: prodi ke fakultas
"""
def mapFacultyDegree():
  mapper = {
    "FS": [
        "biologi",
        "fisika",
        "sains lingkungan kelautan",
        "sains atmosfer dan keplanetan",
        "sains data",
        "farmasi",
        "kimia",
        "aktuaria"
    ],

    "FTI": [
        "teknik pertambangan",
        "teknik elektro",
        "teknik informatika",
        "teknik kimia",
        "teknologi pangan",
        "teknik geologi",
        "rekayasa kosmetik",
        "teknik material",
        "teknik biosistem",
        "teknik biomedis",
        "teknik fisika",
        "teknik geofisika",
        "teknologi industri pertanian"
    ],

    "FTIK": [
        "perencanaan wilayah dan kota",
        "teknik sipil",
        "arsitektur",
        "teknik lingkungan",
        "teknik geomatika",
        "teknik perkeretaapian",
        "desain komunikasi visual"
    ]
}