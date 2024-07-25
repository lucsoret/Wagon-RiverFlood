{{ config(
    materialized='table',
    cluster_by=['code_station']
) }}

WITH top_perc AS (
  SELECT
    code_station,
    grandeur_hydro_elab,
    PERCENTILE_CONT(resultat_obs_elab, 0.99)
      OVER (PARTITION BY code_station) AS quantile_99
  FROM
    `riverflood-lewagon.river_observation_dev.hubeau_historical_bronze`
  WHERE
    grandeur_hydro_elab = 'QmJ'
)

SELECT
  code_station,
  grandeur_hydro_elab,
  AVG(quantile_99) AS quantile_99
FROM
  top_perc
GROUP BY
  code_station
