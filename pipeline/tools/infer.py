from pyspark.sql import functions as F

from pyspark.sql import functions as F

def staging(df):

    rename_map = {
        "No": ("no", "long"),
        "Judul Proposal": ("judul_proposal", "string"),
        "Ketua Peneliti": ("ketua_peneliti", "string"),
        "Jenis": ("jenis", "string"),
        "Status": ("status", "string"),
        "Skema": ("skema", "string"),
        "Scope": ("scope", "string"),
        "SDGs": ("sdgs", "string"),
        "Program Studi": ("program_studi", "string"),
        "Anggota Dosen": ("anggota_dosen", "string"),
        "Anggota Mahasiswa": ("anggota_mahasiswa", "string"),
        "Advisor": ("advisor", "string"),
        "Usulan Biaya": ("usulan_biaya", "long"),
        "Status Proposal": ("status_proposal", "string"),
    }

    existing_cols = set(df.columns)

    for old, (new, dtype) in rename_map.items():
        if old in existing_cols:
            df = df.withColumn(new, F.col(old).cast(dtype)).drop(old)

    return df
    
def inferSchemaDefault(df_dict,spark,sheet_name,start_year=2021,end_year=2026):
    spark_dfs = []
    for tahun in range(start_year, end_year):
        if sheet_name not in df_dict:
            print(f"Sheet not found: {sheet_name}, skipping...")
            continue
        try:
            peng = staging(spark.createDataFrame(df_dict[sheet_name]))
            print(sheet_name, peng.count())
            temp_df = (
                peng
                .withColumn("tahun", F.lit(str(tahun)))
                .withColumn("prodi", get_prodi_udf(F.col("program_studi")))
                .withColumn("fakultas", get_faculty_udf(F.col("program_studi")))
                .withColumn("nim_anggota_mahasiswa", match_unique_id_udf(F.col("anggota_mahasiswa")))
                .withColumn("name_anggota_mahasiswa", match_name_udf(F.col("anggota_mahasiswa")))
                .withColumn("nip_anggota_dosen", match_unique_id_udf(F.col("anggota_dosen")))
                .withColumn("name_anggota_dosen", match_name_udf(F.col("anggota_dosen")))
            )
            spark_dfs.append(temp_df)
        except Exception as e:
            print(f"Error processing {sheet_name}: {e}")
            continue
    if spark_dfs:
        result = spark_dfs[0]
        for sdf in spark_dfs[1:]:
            result = result.unionByName(sdf)
    else:
        result = None
    return result


def staging_sitasi(df):

    rename_map = {
        "No": ("no", "long"),
        "Nama Dosen": ("nama_dosen", "string"),
        "Nama Prodi": ("nama_prodi", "string"),
        "Fakultas": ("fakultas", "string"),
        "Tanggal Terbit": ("tanggal_terbit", "string"),
        "Kategori": ("kategori", "string"),
        "Judul": ("judul", "string"),
        "Sitasi": ("sitasi", "long"),
        "Triwulan": ("triwulan", "long"),
        "Publikasi": ("publikasi", "string"),
        "DOI": ("doi", "string"),
    }

    existing_cols = set(df.columns)

    for old, (new, dtype) in rename_map.items():
        if old in existing_cols:
            df = df.withColumn(new, F.col(f"`{old}`").cast(dtype)).drop(old)

    return df


def inferSchemaSitasi(df_dict, spark, sheet_name):
    if sheet_name not in df_dict:
        print(f"Sheet not found: {sheet_name}, skipping...")
        return None

    try:
        df = staging_sitasi(spark.createDataFrame(df_dict[sheet_name]))
        print(sheet_name, df.count())
        return df
    except Exception as e:
        print(f"Error processing {sheet_name}: {e}")
        return None