from datetime import timedelta
from airflow import DAG
import pendulum
import os
import requests
import json
from google.cloud import storage
from airflow.operators.python import PythonOperator
from hubeau.historical import extract, load
from airflow.decorators import task

CODE_ENTITE = "K*"
client = storage.Client()
temp_file_path = "tempFile"

@task
def extract(date_start = pendulum.now()):
    base_url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/obs_elab"
    params = {
    "date_debut_obs_elab": date_start,
    "date_fin_obs_elab": date_start
    }
    response = requests.get(base_url, params=params)
    # Check if the request was successful
    if response.status_code >= 200 & response.status_code < 300:
        data = response.json()
        print("Data succesfully Extracted")
        return data
    else:
        response.raise_for_status()

@task
def load(data, date_start):
    bucket_name =  os.environ.get("GCP_BUCKET_NAME", "riverflood-lewagon-dev")
    gcs_path_root = 'hubeau_data_historical'
    year, month, day = date_start.split('-')

    target_gcs_path = f"{gcs_path_root}/{year}/{month}/{day}/obs_elab_{date_start}.json"
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(target_gcs_path)
    json_data = json.dumps(data)
    blob.upload_from_string(json_data, content_type='application/json')
    print("Data succesfully Loaded")

# Define the DAG
with DAG(
    "historical_ingestion",
    description="Historical ingestion of Hub'Eau Hydrometrie API",
    schedule_interval= '@daily',
    catchup=True,
    start_date=pendulum.today("UTC").add(days=-1),
    default_args = {"depends_on_past": False}
)  as dag:
    extract_task = extract('{{ ds }}')
    load_task = load(extract_task, '{{ ds }}')
