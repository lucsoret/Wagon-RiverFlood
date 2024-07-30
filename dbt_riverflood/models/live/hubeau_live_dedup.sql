{{ config(
  materialized = 'table',
  cluster_by = ['code_station']
) }}
SELECT  code_site, code_station, grandeur_hydro, date_debut_serie, date_fin_serie, statut_serie, code_systeme_alti_serie, date_obs, resultat_obs, code_methode_obs, libelle_methode_obs, code_qualification_obs, libelle_qualification_obs, continuite_obs_hydro, longitude, latitude
FROM {{ source('riverflood_data', var('GCP_TABLE_LIVE_RAW')) }}
GROUP by  code_site, code_station, grandeur_hydro, date_debut_serie, date_fin_serie, statut_serie, code_systeme_alti_serie, date_obs, resultat_obs, code_methode_obs, libelle_methode_obs, code_qualification_obs, libelle_qualification_obs, continuite_obs_hydro, longitude, latitude
