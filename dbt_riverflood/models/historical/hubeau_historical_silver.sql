{{ config(
    materialized='table',
    cluster_by=['code_station']
) }}

WITH top_perc AS (
  SELECT
    code_station,
    grandeur_hydro_elab,
    PERCENTILE_CONT(resultat_obs_elab, 0.99) OVER (PARTITION BY code_station) AS quantile_99,
    PERCENTILE_CONT(resultat_obs_elab, 0.90) OVER (PARTITION BY code_station) AS quantile_90
  FROM
    {{ ref('hubeau_historical_bronze') }}
  WHERE
      grandeur_hydro_elab = 'QmJ'
    and resultat_obs_elab > 0
)
SELECT
  code_station,
  grandeur_hydro_elab,
  AVG(quantile_99) AS quantile_99,
  AVG(quantile_90) AS quantile_90
FROM
  top_perc
GROUP BY
  code_station,
  grandeur_hydro_elab
