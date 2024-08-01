{{ config(
  materialized = 'view'
) }}
WITH recent_obs AS (
  SELECT
    code_station,
    date_obs,
    grandeur_hydro,
    resultat_obs
  FROM
   {{ ref('hubeau_live_dedup') }}
  WHERE
    date_obs >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    and code_station is not null
)
SELECT
  code_station,
  grandeur_hydro,
  date_obs,
  resultat_obs
FROM
  recent_obs
ORDER BY
  code_station,
  date_obs
