{{ config(
    materialized='incremental',
    cluster_by=['json_file', 'code_station', 'date_obs_elab']
) }}

WITH FlattenedData AS (
  SELECT
    data.latitude,
    data.longitude,
    data.code_qualification,
    data.libelle_statut,
    data.grandeur_hydro_elab,
    data.code_site,
    data.libelle_qualification,
    data.code_statut,
    data.libelle_methode,
    data.code_station,
    data.code_methode,
    data.resultat_obs_elab,
    data.date_obs_elab,
    prev,
    json_file
  FROM
    {{ source('riverflood_data', var('GCP_TABLE_HISTORICAL_RAW')) }},
    UNNEST(data) AS data
)

SELECT *
FROM FlattenedData
{% if is_incremental() %}
    WHERE json_file NOT IN (SELECT DISTINCT json_file FROM {{ this }})
{% endif %}
