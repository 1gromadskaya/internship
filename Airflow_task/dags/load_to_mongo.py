import logging
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
import pandas as pd

# Создаем объект логгера для нашего файла
logger = logging.getLogger(__name__)

INPUT_FILE = '/opt/airflow/data/cleaned_data.csv'
cleaned_dataset = Dataset(f"file://{INPUT_FILE}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
        dag_id='2_load_to_mongo_dag',
        default_args=default_args,
        schedule=[cleaned_dataset],
        catchup=False,
        tags=['loading', 'mongo']
) as dag:
    @task
    def load_csv_to_mongo():
        from pymongo import MongoClient
        import json

        logger.info("1. Reading data from file...")
        df = pd.read_csv(INPUT_FILE)
        data_dict = json.loads(df.to_json(orient='records'))

        logger.info("2. Connecting directly to MongoDB...")
        client = MongoClient("mongodb://admin:password@mongo_db:27017/")

        db = client['analytics_db']
        collection = db['comments']

        logger.info("3. Loading data into the database...")
        if data_dict:
            collection.insert_many(data_dict)
            logger.info(f"Successfully loaded {len(data_dict)} records!")
        else:
            logger.warning("The file is empty, nothing to load.")

    load_csv_to_mongo()