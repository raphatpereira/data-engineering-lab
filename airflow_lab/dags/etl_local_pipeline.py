from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
sys.path.append("/opt/airflow/scripts")

from etl_pipeline import run_pipeline


with DAG(
    dag_id="etl_local_pipeline",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_pipeline
    )
