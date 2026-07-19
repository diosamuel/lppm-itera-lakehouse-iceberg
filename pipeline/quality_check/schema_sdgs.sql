with broken_table as (
    SELECT
        h.id,
        h.skema,
        h.sdgs,
        sm.skema AS skema_match,
        dm.sdgs AS sdgs_match,
        sm2.skema AS sdgs_in_skema,
        dm2.sdgs AS skema_in_sdgs

    FROM hibah_lengkap h
    LEFT JOIN lookup_skema sm
        ON LOWER(TRIM(h.skema)) = LOWER(TRIM(sm.skema))

    LEFT JOIN lookup_sdgs dm
        ON LOWER(TRIM(h.sdgs)) = LOWER(TRIM(dm.sdgs))

    LEFT JOIN lookup_skema sm2
        ON LOWER(TRIM(h.sdgs)) = LOWER(TRIM(sm2.skema))

    LEFT JOIN lookup_sdgs dm2
        ON LOWER(TRIM(h.skema)) = LOWER(TRIM(dm2.sdgs))
    where h.skema is not null and h.sdgs is not null
) select count(*) from broken_table where skema_match is null and sdgs_match is null