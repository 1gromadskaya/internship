from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='airline_snowflake',
    default_args=default_args,
    description='Main 3-Stage Pipeline (RAW -> STAGING -> ANALYTICS)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['snowflake', 'airline'],
) as dag:

    upload_csv_to_stage = SnowflakeOperator(
        task_id='upload_csv_to_stage',
        snowflake_conn_id='snowflake_conn',
        sql="PUT 'file:///opt/airflow/dags/data/Airline Dataset.csv' @AIRLINE_DWH.RAW.AIRLINE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
    )

    load_raw = SnowflakeOperator(
        task_id='load_raw_layer',
        snowflake_conn_id='snowflake_conn',
        sql="CALL AIRLINE_DWH.RAW.LOAD_FLIGHTS_RAW();"
    )

    load_staging = SnowflakeOperator(
        task_id='load_staging_layer',
        snowflake_conn_id='snowflake_conn',
        sql="CALL AIRLINE_DWH.STAGING.LOAD_CLEAN_FLIGHTS();"
    )

    load_analytics = SnowflakeOperator(
        task_id='load_analytics_layer',
        snowflake_conn_id='snowflake_conn',
        sql="CALL AIRLINE_DWH.ANALYTICS.LOAD_FACT_FLIGHTS();"
    )

    upload_csv_to_stage >> load_raw >> load_staging >> load_analytics