import pendulum
import requests
import os
import yaml

from airflow import DAG
from google.cloud import bigquery
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


table_id = f"{os.environ['GCP_PROJECT_ID']}.{os.environ['GCP_DATASET']}.{os.environ['GCP_TABLE_STATIONS']}"
bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
client_bq = bq_hook.get_client()

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


@task
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


@task
def load_to_bq(data):
    with open(
        os.path.join(os.getcwd(),"dags" ,"bigquery", "scd_stations_schema.yml"), 'r'
    ) as file:
        schema = yaml.safe_load(file)

    schema_fields = create_schema_fields(schema)
    job_config = bigquery.LoadJobConfig(
        schema=schema_fields,
        write_disposition="WRITE_TRUNCATE"
    )

    job = client_bq.load_table_from_json(data, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {job.output_rows} rows into {table_id}.")


# Define the DAG
with DAG(
    "sites_ingestion",
    description="Slowly Changing Dimension - Sites",
    schedule_interval= '@weekly',
    catchup=True,
    start_date=pendulum.today("UTC").add(),
    default_args = {"depends_on_past": False}
)  as dag:


    extract_task = extract()
    load_task = load_to_bq(extract_task)

    extract_task >> load_task
