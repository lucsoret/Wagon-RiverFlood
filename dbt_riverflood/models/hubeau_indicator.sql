{{ config(
  materialized = 'view',
  cluster_by = ['code_station']
) }}

WITH latest_metric AS (
  SELECT
    lsilver.code_station,
    lsilver.date_obs,
    lsilver.resultat_obs,
    hsilver.quantile_999,
    hsilver.quantile_990,
    hsilver.quantile_900,
    hsilver.quantile_100,
    hsilver.quantile_010,
    hsilver.quantile_001,
  FROM
    {{ref("hubeau_live_dedup")}} lsilver
  INNER JOIN
    {{ref("hubeau_historical_agg")}} hsilver
  ON
    lsilver.code_station = hsilver.code_station
  WHERE
    lsilver.grandeur_hydro = 'Q'
  ORDER BY
    lsilver.date_obs DESC
)
SELECT
  CASE
    WHEN resultat_obs > quantile_999 then 1
    WHEN resultat_obs < quantile_001 then 0
    else (resultat_obs - quantile_001) / (quantile_999 - quantile_001)
  end as flood_indicateur,
  date_obs,
  resultat_obs,
  code_station,
  quantile_999,
  quantile_990,
  quantile_900,
  quantile_100,
  quantile_010,
  quantile_001,
FROM
  latest_metric
