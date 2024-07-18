import os
import pendulum
import json

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from datetime import datetime

with DAG(
    "live_ingestion",
    description="Live ingestion of Hub'Eau Hydrometrie API",
    schedule_interval = '@daily',
    catchup=True,
    start_date=pendulum.today("UTC").add(days=-1),
    default_args = {"depends_on_past": False}
) as dag:

    start_dataflow_job = DataflowCreatePythonJobOperator(
        task_id="start_dataflow_job",
        py_file='./hubeau/beam.py'
    )
