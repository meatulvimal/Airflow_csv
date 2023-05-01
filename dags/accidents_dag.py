from datetime import datetime, timedelta
from scripts.export_data import export_data
from scripts.extract_data import extract_data
from scripts.transform_data import transform_data
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'accidents_pipeline',
    default_args=default_args,
    description='A simple data pipeline that extracts data from a CSV file, transforms it, and exports it to another CSV file',
    schedule_interval=timedelta(days=1)
)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data.extract_data,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data.transform_data,
    dag=dag
)

export_data = PythonOperator(
    task_id='export_data',
    python_callable=export_data.export_data,
    dag=dag
)

extract_data >> transform_data >> export_data