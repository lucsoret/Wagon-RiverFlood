#from pipeline.schemas import bigquery_schemas
from google.cloud import bigquery
#from google.cloud.exceptions import NotFound
import os


def create_bigquery_table():
    # Define your project ID, dataset ID, and table ID
    #    bigquery_table = 'riverflood-lewagon:river_observations.obs'

    project_id = 'riverflood-lewagon'
    dataset_id = 'river_observations'
    table_id = 'hubeau_observations'

    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the schema based on the provided record structure
    schema = [
        bigquery.SchemaField('code_site', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('code_station', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('grandeur_hydro', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('date_debut_serie', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('date_fin_serie', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('statut_serie', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('code_systeme_alti_serie', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('date_obs', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('resultat_obs', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('code_methode_obs', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('libelle_methode_obs', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('code_qualification_obs', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('libelle_qualification_obs', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('continuite_obs_hydro', 'BOOLEAN', mode='NULLABLE'),
        bigquery.SchemaField('longitude', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('latitude', 'FLOAT', mode='NULLABLE'),

        bigquery.SchemaField('ingestion_timestamp', 'TIMESTAMP', mode='NULLABLE')  # New field
    ]

    # Create the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define the table
    table = bigquery.Table(table_ref, schema=schema)

    # Create the table in BigQuery
    table = client.create_table(table)

    print(f"Table {table_id} created in dataset {dataset_id}.")


if __name__ == "__main__":
    create_bigquery_table()
