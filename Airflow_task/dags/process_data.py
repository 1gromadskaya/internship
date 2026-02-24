from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
import pandas as pd
import os
import re

INPUT_FILE = '/opt/airflow/data/tiktok_google_play_reviews.csv'
OUTPUT_FILE = '/opt/airflow/data/cleaned_data.csv'

cleaned_dataset = Dataset(f"file://{OUTPUT_FILE}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
        dag_id='1_process_data_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        tags=['etl', 'pandas']
) as dag:
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=INPUT_FILE,
        poke_interval=10,
        timeout=600,
        mode='poke'
    )


    def check_file_size():
        if os.path.getsize(INPUT_FILE) > 0:
            return 'processing_group.clean_nulls'
        return 'log_empty'


    branch_task = BranchPythonOperator(
        task_id='check_if_empty',
        python_callable=check_file_size
    )

    log_empty = BashOperator(
        task_id='log_empty',
        bash_command='echo "File is empty!"'
    )


    @task_group(group_id='processing_group')
    def processing_tasks():

        @task
        def clean_nulls():
            df = pd.read_csv(INPUT_FILE)
            df.fillna('-', inplace=True)
            df.to_csv(OUTPUT_FILE, index=False)

        @task
        def sort_data():
            df = pd.read_csv(OUTPUT_FILE)
            if 'created_date' in df.columns:
                df['created_date'] = pd.to_datetime(df['created_date'])
                df = df.sort_values(by='created_date')
            df.to_csv(OUTPUT_FILE, index=False)

        @task(outlets=[cleaned_dataset])
        def clean_content():
            df = pd.read_csv(OUTPUT_FILE)

            def clean_text(text):
                if not isinstance(text, str):
                    return text
                return re.sub(r'[^\w\s\.,!?-]', '', text)

            if 'content' in df.columns:
                df['content'] = df['content'].apply(clean_text)

            df.to_csv(OUTPUT_FILE, index=False)

        clean_nulls() >> sort_data() >> clean_content()


    wait_for_file >> branch_task
    branch_task >> log_empty
    branch_task >> processing_tasks()