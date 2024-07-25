{{ config(
  materialized = 'table'
) }}
SELECT
  code_station,
  EXTRACT(MONTH FROM date_obs) AS month,
  AVG(resultat_obs) AS avg_resultat_obs
FROM
  {{ ref('hubeau_live_dedup') }}
  where grandeur_hydro = 'H'
GROUP BY
  code_station,
  month
ORDER BY
  code_station,
  month
