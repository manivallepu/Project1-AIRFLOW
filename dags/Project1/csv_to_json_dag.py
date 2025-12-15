from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from common.utils.csv_to_json import csv_to_json


def run_csv_to_json():
    csv_path = "/opt/airflow/dags/project1/sample.csv"
    json_path = "/opt/airflow/dags/project1/output.json"

    csv_to_json(csv_path, json_path)


with DAG(
    dag_id="project1_csv_to_json_poc",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["project1", "poc"],
) as dag:

    convert_task = PythonOperator(
        task_id="convert_csv_to_json",
        python_callable=run_csv_to_json
    )
