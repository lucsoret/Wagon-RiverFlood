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
    "dbt_historical",
    description="Historical ingestion of Hub'Eau Hydrometrie API",
    schedule_interval= '0 20 * * *',
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
    dbt_run_bronze_task = make_dbt_task("hubeau_historical_bronze", "run")
    dbt_test_bronze_task = make_dbt_task("hubeau_historical_bronze", "test")
    dbt_run_silver_task = make_dbt_task("hubeau_historical_silver", "run")
    dbt_test_silver_task = make_dbt_task("hubeau_historical_silver", "test")

    start_task >> dbt_run_bronze_task >> dbt_test_bronze_task >> dbt_run_silver_task >> dbt_test_silver_task >> end_task
