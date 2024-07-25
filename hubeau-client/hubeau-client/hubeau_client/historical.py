from datetime import timedelta
from airflow import DAG
import pendulum
import os
import requests
import json
import yaml
from google.cloud import storage
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

CODE_ENTITE = "K*"
credentials_info = json.loads(os.environ['GCP_CONNEXION_STRING'])
client_gcs = storage.Client()
client_bq = bigquery.Client()
credentials = service_account.Credentials.from_service_account_info(credentials_info)
client_bq = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])

temp_file_path = "tempFile"
bucket_name =  os.environ.get("GCP_BUCKET_NAME", "riverflood-lewagon-dev")
dataset_id = os.environ.get("GCP_DATASET", 'river_observation_multiregion')
table_id = os.environ.get("GCP_TABLE_HISTORICAL_RAW", 'hubeau_historical')

def create_bq_table_if_not_exists():
    with open(
        os.path.join(
            os.getcwd(),
            "bigquery", "historical_bronze_schema.yml"), 'r'
    ) as file:
        schema = yaml.safe_load(file)

    # Convert schema into BigQuery SchemaField format
    schema_fields = [
        bigquery.SchemaField(field['name'], field['type'], mode=field['mode'], description=field['description'], fields=[
            bigquery.SchemaField(sub_field['name'], sub_field['type'], mode=sub_field['mode'], description=sub_field['description'])
            for sub_field in field.get('fields', [])
        ]) if field['type'] == 'RECORD' else bigquery.SchemaField(field['name'], field['type'], mode=field['mode'], description=field['description'])
        for field in schema
    ]

    table_ref = client_bq.dataset(dataset_id).table(table_id)

    try:
        table = client_bq.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except:
        # Table does not exist, create it
        table = bigquery.Table(table_ref, schema=schema_fields)
        table = client_bq.create_table(table)
        print(f"Created table {table_id}.")

def check_json_in_table(json_file):
    query_str = f"""
        SELECT * FROM `{dataset_id}.{table_id}`
        where json_file = '{json_file}'
        """
    query_job = client_bq.query(query_str)  # Make an API request.
    results = query_job.result()  # Wait for the query to finish
    ar = [r for r in results]
    L = len(ar)
    if L >= 1:
        if len(ar) >= 1:
            print(f'[WARNING] JSON file {json_file} has {L} entry in the db')
        print(f'JSON file {json_file} already exists in the db... skipped')
        return False
    return True

def load_gcs_to_bq(gcs_path):

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
    print(f"Loaded {destination_table.num_rows} rows into {dataset_id}:{table_id}.")


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

def load_to_gcs(data, date_start = pendulum.now()):
    if data:
        gcs_path_root = 'hubeau_data_historical'
        year, month, day = date_start.split('-')
        target_gcs_path = f"{gcs_path_root}/{year}/{data['json_file']}"
        json_data = json.dumps(data)

        bucket = client_gcs.get_bucket(bucket_name)
        blob = bucket.blob(target_gcs_path)
        blob.upload_from_string(json_data)
        return target_gcs_path

def gcs_to_bq(gcs_path):
    if gcs_path:
        # Create the table if not exists, with my pre-defined schema
        create_bq_table_if_not_exists()
        # Check if the json file already exists in the table, (so consider to add the json file information)
        condition = check_json_in_table(gcs_path.split("/")[-1])

        # If not, append the data
        if condition:
            load_gcs_to_bq(gcs_path)

if __name__ == "__main__":
    for tdelta in range(365):
        for param in ['QmJ', 'QmM']:
            dt = (datetime.today() - timedelta(days=tdelta)).strftime('%Y-%m-%d')
            js = extract(dt, param=param)
            gcs_path = load_to_gcs(js, date_start=dt)
            gcs_to_bq(gcs_path)
