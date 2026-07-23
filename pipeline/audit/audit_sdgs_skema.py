import pandas as pd
import trino


conn = trino.dbapi.connect(
    host="lppm-trino",
    port=8085,
    user="jpyt",
    catalog="default",
)
cursor = conn.cursor()
cursor.execute("USE default.silver")


def query(sql):
    curr = cursor.execute(sql)

    if curr.description is None:
        return None

    rows = curr.fetchall()
    columns = [col[0] for col in curr.description]
    return pd.DataFrame(rows, columns=columns)


def scalar(sql):
    result = query(sql)
    return int(result.iat[0, 0])


AUDIT_SQL = """
WITH checked AS (
    SELECT
        h.id,
        h.skema,
        h.sdgs,
        sm.nama_skema AS skema_match,
        dm.kode_sdgs AS sdgs_match,
        sm2.nama_skema AS sdgs_is_skema,
        dm2.kode_sdgs AS skema_is_sdgs
    FROM hibah_lengkap h
    LEFT JOIN gold.dim_skema sm
        ON LOWER(TRIM(h.skema)) = LOWER(TRIM(sm.nama_skema))
    LEFT JOIN gold.dim_sdgs dm
        ON LOWER(TRIM(h.sdgs)) = LOWER(TRIM(dm.kode_sdgs))
    LEFT JOIN gold.dim_skema sm2
        ON LOWER(TRIM(h.sdgs)) = LOWER(TRIM(sm2.nama_skema))
    LEFT JOIN gold.dim_sdgs dm2
        ON LOWER(TRIM(h.skema)) = LOWER(TRIM(dm2.kode_sdgs))
)
SELECT COUNT(*)
FROM checked
WHERE
    (
        skema_match IS NULL
        AND sdgs_match IS NULL
        AND skema_is_sdgs IS NOT NULL
        AND sdgs_is_skema IS NOT NULL
    )
    OR (
        skema IS NULL
        AND sdgs_is_skema IS NOT NULL
        AND skema_is_sdgs IS NULL
    )
    OR (
        sdgs IS NULL
        AND skema_is_sdgs IS NOT NULL
        AND sdgs_is_skema IS NULL
    )
"""


MERGE_SQL = """
MERGE INTO hibah_lengkap AS t
USING (
    SELECT
        h.id,
        h.skema,
        h.sdgs,
        sm.nama_skema AS skema_match,
        dm.kode_sdgs AS sdgs_match,
        sm2.nama_skema AS sdgs_is_skema,
        dm2.kode_sdgs AS skema_is_sdgs
    FROM hibah_lengkap AS h
    LEFT JOIN gold.dim_skema AS sm
        ON LOWER(TRIM(h.skema)) = LOWER(TRIM(sm.nama_skema))
    LEFT JOIN gold.dim_sdgs AS dm
        ON LOWER(TRIM(h.sdgs)) = LOWER(TRIM(dm.kode_sdgs))
    LEFT JOIN gold.dim_skema AS sm2
        ON LOWER(TRIM(h.sdgs)) = LOWER(TRIM(sm2.nama_skema))
    LEFT JOIN gold.dim_sdgs AS dm2
        ON LOWER(TRIM(h.skema)) = LOWER(TRIM(dm2.kode_sdgs))
) AS s
ON t.id = s.id
WHEN MATCHED
    AND s.skema_match IS NULL
    AND s.sdgs_match IS NULL
    AND s.skema_is_sdgs IS NOT NULL
    AND s.sdgs_is_skema IS NOT NULL
THEN UPDATE SET
    skema = s.sdgs,
    sdgs = s.skema
WHEN MATCHED
    AND s.skema IS NULL
    AND s.sdgs_is_skema IS NOT NULL
    AND s.skema_is_sdgs IS NULL
THEN UPDATE SET
    skema = s.sdgs,
    sdgs = NULL
WHEN MATCHED
    AND s.sdgs IS NULL
    AND s.skema_is_sdgs IS NOT NULL
    AND s.sdgs_is_skema IS NULL
THEN UPDATE SET
    skema = NULL,
    sdgs = s.skema
"""


before_count = scalar(AUDIT_SQL)

if before_count > 0:
    query(MERGE_SQL)

after_count = scalar(AUDIT_SQL)
print(f"sdgs/skema rows needing repair before merge: {before_count}")
print(f"sdgs/skema rows needing repair after merge: {after_count}")
