from datetime import timedelta
from airflow import DAG
import pendulum
import os
import requests
import json
from google.cloud import storage
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

CODE_ENTITE = "K*"
client = storage.Client()
temp_file_path = "tempFile"

def extract(date_start = pendulum.now()):
    base_url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/obs_elab"
    params = {
    "date_debut_obs_elab": date_start,
    "date_fin_obs_elab": date_start,
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
        print(f"count [{size}]  | size [{len(response.json()['data'])}]")
        breakpoint()
        return response.json()
    else:
        response.raise_for_status()

def json_to_gcs(data, date_start = pendulum.now()):
    if data:
        bucket_name =  os.environ.get("GCP_BUCKET_NAME", "riverflood-lewagon-dev")
        gcs_path_root = 'hubeau_data_historical'
        year, month, day = date_start.split('-')

        target_gcs_path = f"{gcs_path_root}/{year}/obs_elab_{date_start}.json"
        json_data = json.dumps(data)

        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(target_gcs_path)
        blob.upload_from_string(json_data)

#def gcs_to_bq(fuck_you, data_start = pendulum.now())


if __name__ == "__main__":
    for tdelta in range(365):
        dt = (datetime.today() - timedelta(days=tdelta)).strftime('%Y-%m-%d')
        j = extract(dt)
        json_to_gcs(j, date_start=dt)
