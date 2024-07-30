from datetime import timedelta
from airflow import DAG
import pendulum
import os
import requests
import json
import yaml
import logging
from google.cloud import storage
from google.cloud import bigquery
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from utils.utils import create_schema_fields
from airflow.models import Variable

temp_file_path = "tempFile"
bucket_name = Variable.get("GCP_BUCKET_NAME")
dataset_id = Variable.get("GCP_DATASET")
table_id = Variable.get("GCP_TABLE_HISTORICAL_RAW")

gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
client_bq = bq_hook.get_client()

logger = logging.getLogger(__name__)

@task
def finish():
    return 'Finito'

@task
def create_bq_table_if_not_exists():
    with open(
        os.path.join(os.getcwd(),"dags" ,"bigquery", "historical_bronze_schema.yml"), 'r'
    ) as file:
        schema = yaml.safe_load(file)

    # Convert schema into BigQuery SchemaField format
    schema_fields = create_schema_fields(schema)

    table_ref = client_bq.dataset(dataset_id).table(table_id)

    try:
        table = client_bq.get_table(table_ref)
        logger.info(f"Table {table_id} already exists.")
    except:
        # Table does not exist, create it
        table = bigquery.Table(table_ref, schema=schema_fields)
        table = client_bq.create_table(table)
        logger.info(f"Created table {table_id}.")
    return None

@task.branch
def check_json_in_table(gcs_path):
    if gcs_path:
        json_file = gcs_path.split("/")[-1]
        query_str = f"""
            SELECT * FROM `{dataset_id}.{table_id}`
            WHERE json_file = '{json_file}'
            """
        logger.info(query_str)
        query_job = client_bq.query(query_str)  # Make an API request
        results = query_job.result()  # Wait for the query to finish
        ar = [r for r in results]
        L = len(ar)
        if L >= 1:
            if len(ar) >= 1:
                logger.warn(f'[WARNING] JSON file {json_file} has {L} entry in the db')
            logger.info(f'JSON file {json_file} already exists in the db... skipped')
            return 'finish'  # If JSON exists, end the workflow
        return 'load_gcs_to_bq'  # If JSON does not exist, continue to load_gcs_task
    return 'finish'  # If no gcs_path, end the workflow


@task
def load_gcs_to_bq(gcs_path):
    if gcs_path:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,  # Set to False since we are providing schema
        )
        uri = f"gs://{bucket_name}/{gcs_path}"
        load_job = client_bq.load_table_from_uri(
            uri, f"{dataset_id}.{table_id}", job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.
        destination_table = client_bq.get_table(f"{dataset_id}.{table_id}")
        logger.info(f"Loaded {destination_table.num_rows} rows into {dataset_id}:{table_id}.")


@task
def extract(date_start, param='QmJ'):
    base_url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/obs_elab"
    params = {
    "date_debut_obs_elab": date_start,
    "date_fin_obs_elab": date_start,
    'grandeur_hydro_elab': param,
    "size": 1
    }
    response = requests.get(base_url, params=params)
    # Check if the request was successful
    if response.status_code >= 200 & response.status_code < 300:
        data = response.json()
        if "count" not in data.keys():
            return None
        if data["count"] == 0:
            return None
        size = data["count"]

        params["size"] = size
        response = requests.get(base_url, params=params)
        js = response.json()
        js["json_file"] = f'obs_elab_{param}_{date_start}.json'
        return js
    else:
        response.raise_for_status()

@task
def load_to_gcs(data, date_start = pendulum.now()):
    if data:
        gcs_path_root = 'hubeau_data_historical'
        year, month, day = date_start.split('-')

        target_gcs_path = f"{gcs_path_root}/{year}/{data['json_file']}"
        json_data = json.dumps(data)
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=target_gcs_path,
            data=json_data
        )
        logger.info("Data succesfully Loaded")
        return target_gcs_path

# Define the DAG
with DAG(
    "historical_ingestion",
    description="Historical ingestion of Hub'Eau Hydrometrie API",
    schedule_interval= '0 20 * * *',
    catchup=True,
    start_date=pendulum.today("UTC").add(days=-1),
    default_args = {"depends_on_past": False}
)  as dag:

    finish_task = finish()
    extract_task = extract('{{ macros.ds_add(ds, -1) }}')
    load_task = load_to_gcs(extract_task, '{{ macros.ds_add(ds, -1) }}')
    create_bq_table_task = create_bq_table_if_not_exists()
    check_json_task = check_json_in_table(load_task)

    load_gcs_task = load_gcs_to_bq(load_task)

    # Task dependencies
    create_bq_table_task >> extract_task >> load_task >> check_json_task
    load_gcs_task >> finish_task
    check_json_task >> [load_gcs_task, finish_task]
