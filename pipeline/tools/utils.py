import re

"""
matchNIP: Nomor Induk Pegawai
"""
def matchNIP(text):
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