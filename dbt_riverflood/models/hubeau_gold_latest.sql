{{ config(
  materialized = 'view',
  cluster_by = ['code_station']
) }}

WITH latest_metric AS (
  SELECT
    lsilver.code_station,
    lsilver.date_obs,
    lsilver.resultat_obs,
    hsilver.quantile_99,
    hsilver.quantile_90,
    hsilver.quantile_10,
    hsilver.quantile_01,
  FROM
    {{ref("hubeau_live_latest")}} lsilver
  INNER JOIN
    {{ref("hubeau_historical_silver")}} hsilver
  ON
    lsilver.code_station = hsilver.code_station
  WHERE
    lsilver.grandeur_hydro = 'Q'
  ORDER BY
    lsilver.date_obs DESC
)
SELECT
  CASE
    WHEN resultat_obs > quantile_99 then 1
    WHEN resultat_obs < quantile_01 then 0
    else (resultat_obs - quantile_01) / (quantile_99 - quantile_01)
  end as flood_indicateur,
  date_obs,
  resultat_obs,
  code_station,
  quantile_01,
  quantile_10,
  quantile_90,
  quantile_99,
FROM
  latest_metric
