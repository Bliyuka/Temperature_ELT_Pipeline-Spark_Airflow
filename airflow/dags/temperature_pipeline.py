import sys
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator # Less flexible for for-loop style
from datetime import datetime, timedelta

# Add the modules folder to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules')))
# Modules
from python_modules import check_new_csv, load_csv_to_mysql
# import pyspark_load_visualize

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'temperature_data_pipeline',
    default_args=default_args,
    description='ELT pipeline to process temperature data',
    # schedule_interval='@daily',
    schedule_interval="* * * * *"
)

# Start Task
start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
)

# End Task
end_task = EmptyOperator(
    task_id='end_task',
    dag=dag,
)

# Sensor to watch for new CSV files
file_sensor = PythonSensor(
    task_id="watch_for_new_csv",
    python_callable=check_new_csv,
    op_kwargs={"watch_dir": "/media/Personal/Project/Temperature_Pipeline/data/ingested_data", 
               "processed_files": []},
    provide_context=True,
    poke_interval=30,
    timeout=120,
    dag=dag,
)

# Load CSV to MySQL
load_to_mysql = PythonOperator(
    task_id='load_csv_to_mysql_sequentially',
    python_callable=load_csv_to_mysql,
    op_args=[
        "{{ ti.xcom_pull(task_ids='watch_for_new_csv', key='new_files') }}"
    ],
    provide_context=True,
    dag=dag,
)

# Running Spark jon
run_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit /media/Personal/Project/Temperature_Pipeline/modules/pyspark_load_visualize.py',
    dag=dag
)

# DAG flow
start_task >> file_sensor >> load_to_mysql >> run_spark_job >> end_task
