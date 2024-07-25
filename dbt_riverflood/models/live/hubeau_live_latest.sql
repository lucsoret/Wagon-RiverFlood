{{ config(
  materialized = 'table',
  cluster_by = ['code_station']
) }}
WITH ranked_rows AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY code_station ORDER BY date_obs DESC) AS row_num
  FROM
   {{ ref('hubeau_live_dedup') }}
)
SELECT
  *
FROM ranked_rows
WHERE
  row_num = 1
