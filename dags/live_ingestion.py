import json
from datetime import datetime, timedelta, timezone

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery


@dag(
    schedule_interval='0 */4 * * *',
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=True,
    default_args={'owner': 'airflow', 'depends_on_past': False},
    description="Live ingestion of Hub'Eau Hydrometrie API",
    concurrency=1
)
def live_ingestion():
    gcp_bucket_name = Variable.get("GCP_BUCKET_NAME")
    gcp_dataset = Variable.get("GCP_DATASET")
    gcp_table_live_raw = Variable.get("GCP_TABLE_LIVE_RAW")

    gcs_hook = GCSHook()
    bq_hook = BigQueryHook(use_legacy_sql=False)

    bq_client = bq_hook.get_client()
    gcs_client = gcs_hook.get_conn()

    @task
    def fetch_hubeau_data():
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=4)
        date_debut_obs = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

        url = f"https://hubeau.eaufrance.fr/api/v1/hydrometrie/observations_tr?code_site=K1370003&date_debut_obs={date_debut_obs}"

        response = requests.get(url)
        if response.status_code >= 200 and response.status_code < 300:
            data = response.json()
            return data
        else:
            return []

    @task
    def write_to_gcs(data):
        if not data:
            return

        file_name = f"/tmp/hubeau_data_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
        with open(file_name, 'w') as f:
            for record in data['data']:
                f.write(json.dumps(record) + '\n')

        bucket = gcs_client.get_bucket(gcp_bucket_name)
        blob = bucket.blob(f"hubeau_data/{file_name.split('/')[-1]}")
        blob.upload_from_filename(file_name)

        return blob.name

    @task
    def read_from_gcs_and_prepare_for_bq(blob_name):
        if not blob_name:
            return

        bucket = gcs_client.get_bucket(gcp_bucket_name)
        blob = bucket.blob(blob_name)

        file_name = f"/tmp/bq_ready_data_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
        with open(file_name, 'w') as f:
            for line in blob.download_as_string().decode().split('\n'):
                if line:
                    record = json.loads(line)
                    record['ingestion_timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                    f.write(json.dumps(record) + '\n')

        return file_name

    @task
    def upload_prepared_data_to_gcs(file_name):
        if not file_name:
            return

        bucket = gcs_client.get_bucket(gcp_bucket_name)
        blob = bucket.blob(f"prepared_data/{file_name.split('/')[-1]}")
        blob.upload_from_filename(file_name)

        return blob.name

    @task
    def load_to_bigquery(prepared_gcs_file_name):
        if not prepared_gcs_file_name:
            return

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            schema=[
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
                bigquery.SchemaField('ingestion_timestamp', 'TIMESTAMP', mode='NULLABLE'),
            ]
        )
        uri = f"gs://{gcp_bucket_name}/{prepared_gcs_file_name}"
        load_job = bq_client.load_table_from_uri(
            uri, f"{gcp_dataset}.{gcp_table_live_raw}", job_config=job_config
        )

        load_job.result()

    data = fetch_hubeau_data()
    gcs_file_name = write_to_gcs(data)
    bq_ready_file_name = read_from_gcs_and_prepare_for_bq(gcs_file_name)
    prepared_gcs_file_name = upload_prepared_data_to_gcs(bq_ready_file_name)
    load_to_bigquery(prepared_gcs_file_name)


live_ingestion_dag = live_ingestion()
