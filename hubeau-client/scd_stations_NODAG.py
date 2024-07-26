import requests
import pandas as pd
import os
import yaml
import json
from io import StringIO
from google.cloud import bigquery

client = bigquery.Client()
table_id = f"{os.environ['GCP_PROJECT_ID']}.{os.environ['GCP_DATASET']}.{os.environ['GCP_TABLE_STATIONS']}"

def create_schema_fields(schema):
    def create_field(field):
        # Default mode and description if not provided
        mode = field.get('mode', 'NULLABLE')
        description = field.get('description', None)

        if field['type'] == 'RECORD':
            # Recursively process sub-fields
            sub_fields = [
                create_field(sub_field)
                for sub_field in field.get('fields', [])
            ]
            return bigquery.SchemaField(
                field['name'],
                'RECORD',
                mode=mode,
                description=description,
                fields=sub_fields
            )
        else:
            return bigquery.SchemaField(
                field['name'],
                field['type'],
                mode=mode,
                description=description
            )

    return [create_field(field) for field in schema]

def extract():
    # Define the URL and parameters
    base_url = 'https://hubeau.eaufrance.fr/api/v1/hydrometrie/referentiel/stations'
    params = {
        'format': 'json',
        "size": 10000
    }
    # Define the headers
    headers = {
        'accept': 'application/json'
    }
    response = requests.get(base_url, params=params, headers=headers)
    # Check if the request was successful
    if response.status_code >= 200 & response.status_code < 300:
        return response.json()["data"]
    else:
        response.raise_for_status()

def load_to_bq(data):
    with open(
        os.path.join(os.getcwd(),"dags" ,"bigquery", "scd_stations_schema.yml"), 'r'
    ) as file:
        schema = yaml.safe_load(file)

    schema_fields = create_schema_fields(schema)
    job_config = bigquery.LoadJobConfig(
        schema=schema_fields,
        write_disposition="WRITE_TRUNCATE",  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    )
    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {job.output_rows} rows into {table_id}.")

if __name__ == '__main__':
    # TODO : CHANGE THE WORKFLOW, FIRST TO GCS THEN TO BQ
    data = extract()
    load_to_bq(data)
