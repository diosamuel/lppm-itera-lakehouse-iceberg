# Superset

```sql
SELECT `status` AS `status`, sum(`total`) AS `SUM(total)` 
FROM (SELECT count(*) AS total,
       status
FROM default.penelitian where status is not null 
GROUP BY status
) AS `virtual_table` GROUP BY `status` ORDER BY `SUM(total)` DESC
 LIMIT 10000;
```

```sql
SELECT `tahun` AS `tahun`, `status` AS `status`, sum(`total`) AS `SUM(total)` 
FROM (SELECT count(*) AS total,
       status,tahun
FROM default.penelitian where status is not null 
GROUP BY status,tahun
) AS `virtual_table` GROUP BY `tahun`, `status` ORDER BY `SUM(total)` DESC
 LIMIT 10000;
```
