import os
import logging
import pandas as pd
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "superstore_db")

DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)


def execute_sql_file(filepath, engine_conn):
    with open(filepath, 'r', encoding='utf-8') as file:
        sql_script = file.read()

    with engine_conn.begin() as conn:
        conn.execute(text(sql_script))
    logging.info(f"Executed: {filepath}")


def main():
    try:
        execute_sql_file('sql/01_init_ddl.sql', engine)

        csv_path = 'data/Sample - Superstore.csv'
        df = pd.read_csv(csv_path, encoding='windows-1252')

        df.to_sql(
            name='superstore_raw',
            schema='stage',
            con=engine,
            if_exists='replace',
            index=False
        )
        logging.info(f"Loaded {len(df)} rows to stage.superstore_raw")

        execute_sql_file('sql/02_transform.sql', engine)

        logging.info("ETL Pipeline completed successfully")

    except Exception as e:
        logging.error(f"ETL Pipeline failed: {e}")


if __name__ == "__main__":
    main()