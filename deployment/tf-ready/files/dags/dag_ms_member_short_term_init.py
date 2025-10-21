"""
Placeholder DAG for ms_member_short_term_init.
Replace with your actual Airflow DAG definition.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def dummy_task():
    print("This is a placeholder DAG.")

with DAG(
    dag_id="dag_ms_member_short_term_init",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    run = PythonOperator(
        task_id="run",
        python_callable=dummy_task
    )