import os
import pendulum

import pandas as pd
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

with DAG(
    "live_ingestion",
    description="Live ingestion of Hub'Eau Hydrometrie API",
    schedule_interval = '@daily',
    catchup=True,
    start_date=pendulum.today("UTC").add(days=-1),
    default_args = {"depends_on_past": False}
) as dag:

    # Don't forget to add conn id to AirFlow (https://kitt.lewagon.com/camps/1609/challenges?path=03-Data-Storage-and-Batch-Pipelines%2F03-Pipeline-Orchestration%2F01-Advanced-Airflow)
    # Make sure to make it one line
    start_dataflow_job = DataflowCreatePythonJobOperator(
        task_id="start_dataflow_job",
        py_file='/opt/airflow/dags/hubeau/beam.py',
        gcp_conn_id='google_cloud_default',
        dataflow_default_options={
            'project': os.environ.get("GCP_PROJECT_ID", "riverflood-lewagon"),
            'region': os.environ.get("GCP_REGION", "europe-west1-d"),
            'staging_location': 'gs://riverflood-lewagon-dev-temp/dataflow/staging',
            'temp_location': 'gs://riverflood-lewagon-dev-temp/dataflow/staging',
            'runner': 'DataflowRunner',
            'job_name': 'live-ingestion-job-{{ ds_nodash }}',
            'requirements_file': '/opt/airflow/requirements.txt'
        }
    )
