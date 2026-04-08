from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
sys.path.append("/opt/airflow/scripts")

from etl_pipeline import extract, transform, load


with DAG(

    dag_id="etl_local_pipeline",

    start_date=datetime(2024,1,1),

    schedule="@daily",

    catchup=False,

    tags=["etl"]

) as dag:


    extract_task = PythonOperator(

        task_id="extract",

        python_callable=extract

    )


    transform_task = PythonOperator(

        task_id="transform",

        python_callable=transform,

        op_args=["{{ ti.xcom_pull(task_ids='extract') }}"]

    )


    load_task = PythonOperator(

        task_id="load",

        python_callable=load,

        op_args=["{{ ti.xcom_pull(task_ids='transform') }}"]

    )


    extract_task >> transform_task >> load_task
