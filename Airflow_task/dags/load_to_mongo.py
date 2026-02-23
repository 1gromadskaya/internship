from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
import pandas as pd

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

        print("1. Чтение данных из файла...")
        df = pd.read_csv(INPUT_FILE)
        data_dict = json.loads(df.to_json(orient='records'))

        print("2. Подключение к MongoDB напрямую...")
        client = MongoClient("mongodb://admin:password@mongo_db:27017/")

        db = client['analytics_db']
        collection = db['comments']

        print("3. Загрузка данных в базу...")
        if data_dict:
            collection.insert_many(data_dict)
            print(f"Успешно загружено {len(data_dict)} записей!")
        else:
            print("Файл пуст, загружать нечего.")


    load_csv_to_mongo()