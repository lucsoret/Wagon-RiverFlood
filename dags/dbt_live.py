from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pendulum

DBT_DIR = '/opt/airflow/dbt_riverflood'
def make_dbt_task(model: str, dbt_verb: str) -> BashOperator:
    return BashOperator(
        task_id=f"{model}-{dbt_verb}",
        bash_command=(f"dbt {dbt_verb} --models {model} --project-dir {DBT_DIR}  --profiles-dir {DBT_DIR}"),
    )

with DAG(
    "dbt_live",
    description="Live ingestion of Hub'Eau Hydrometrie API",
    schedule_interval = '*/10 * * * *',
    catchup=True,
    start_date=pendulum.today("UTC").add(days=-1),
    default_args = {"depends_on_past": False}
)  as dag:
    start_task = EmptyOperator(
    task_id='start',
    )
    end_task = EmptyOperator(
        task_id='end',
    )
    dbt_run_dedup_task = make_dbt_task("hubeau_live_dedup", "run")
    #dbt_test_bronze_task = make_dbt_task("hubeau_historical_bronze", "test")
    dbt_run_latest_task = make_dbt_task("hubeau_live_latest", "run")
    #dbt_test_silver_task = make_dbt_task("hubeau_historical_silver", "test")
    dbt_run_avg_task = make_dbt_task("hubeau_live_avg", "run")
    dbt_run_gold_task = make_dbt_task("hubeau_gold", "run")
    dbt_run_gold_latest_task =  make_dbt_task("hubeau_gold_latest", "run")

    start_task >> dbt_run_dedup_task >> [dbt_run_latest_task, dbt_run_avg_task, dbt_run_gold_task] >> end_task
    dbt_run_latest_task >> dbt_run_gold_latest_task >> end_task
    #>> dbt_test_bronze_task >> dbt_run_silver_task >> dbt_test_silver_task
