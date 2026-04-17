from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "zulham-tech", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id='data_quality_validation',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=['data-quality'],
) as dag:
    validate_raw     = BashOperator(task_id='validate_raw',     bash_command='great_expectations checkpoint run raw_checkpoint')
    validate_staging = BashOperator(task_id='validate_staging', bash_command='great_expectations checkpoint run staging_checkpoint')
    publish_docs     = BashOperator(task_id='publish_docs',     bash_command='great_expectations docs build --no-view')
    validate_raw >> validate_staging >> publish_docs
