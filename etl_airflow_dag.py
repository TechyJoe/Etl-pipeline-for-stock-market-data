from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl import etl_pipeline  # Import ETL function

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for stock market data',
    schedule_interval=timedelta(days=1),  # Runs daily
)

run_etl = PythonOperator(
    task_id='run_stock_etl',
    python_callable=etl_pipeline,
    op_args=['AAPL'],
    dag=dag,
)

run_etl
