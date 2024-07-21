import json
import requests
import os
from datetime import datetime, timedelta
from google.cloud import storage

CODE_ENTITE = "K*"
client = storage.Client()
temp_file_path = "tempFile"

def extract(date_start = datetime.now()):
    base_url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/obs_elab"
    params = {
    "date_debut_obs_elab": date_start.strftime("%Y-%m-%d"),
    "date_fin_obs_elab": ((date_start + timedelta(days=0)).strftime("%Y-%m-%d"))
    }
    response = requests.get(base_url, params=params)
    # Check if the request was successful
    if response.status_code >= 200 & response.status_code < 300:
        data = response.json()
        print("Data succesfully Extracted")
        return data
    else:
        response.raise_for_status()


def run(date_start = datetime.now()):
    date_start = datetime(2024,1,1)
    data = extract(date_start=date_start)
    load(data, date_start)

def load(data, date_start):
    bucket_name =  os.environ.get("GCP_BUCKET_NAME", "riverflood-lewagon-dev")
    gcs_path_root = 'hubeau_data_historical'
    year, month, day = date_start.strftime("%Y"), date_start.strftime("%m"), date_start.strftime("%d")

    target_gcs_path = f"{gcs_path_root}/{year}/{month}/{day}/obs_elab_{date_start.strftime('%Y%m%d%H%M%S')}.json"
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(target_gcs_path)
    json_data = json.dumps(data)
    blob.upload_from_string(json_data, content_type='application/json')
    print("Data succesfully Loaded")

if __name__ == "__main__":
    run()
