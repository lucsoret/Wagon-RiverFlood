{{ config(
    materialized='table',
    cluster_by=['code_station']
) }}

SELECT
  code_station,
  latitude,
  longitude,
  grandeur_hydro_elab,
  APPROX_QUANTILES(resultat_obs_elab, 1000)[OFFSET(999)]/10 AS quantile_999,
  APPROX_QUANTILES(resultat_obs_elab, 1000)[OFFSET(990)]/10 AS quantile_990,
  APPROX_QUANTILES(resultat_obs_elab, 1000)[OFFSET(900)]/10 AS quantile_900,
  APPROX_QUANTILES(resultat_obs_elab, 1000)[OFFSET(100)]/10 AS quantile_100,
  APPROX_QUANTILES(resultat_obs_elab, 1000)[OFFSET(10)]/10 AS quantile_010,
  APPROX_QUANTILES(resultat_obs_elab, 1000)[OFFSET(1)]/10 AS quantile_001,

  min(date_obs_elab) as minimum_date_window,
  max(date_obs_elab) as maximum_date_window
FROM
  {{ref('hubeau_historical_flatten')}}
WHERE
  grandeur_hydro_elab = 'QmJ'
  AND resultat_obs_elab > 0
GROUP BY
  code_station,
  latitude,
  longitude,
  grandeur_hydro_elab
