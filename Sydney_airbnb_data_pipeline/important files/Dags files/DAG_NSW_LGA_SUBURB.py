import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Constants
BUCKET_NAME = 'australia-southeast1-bdeenv-29f99fa6-bucket'
OBJECT_NAME = 'data/NSW_LGA_SUBURB.csv'
LOCAL_FILE = '/tmp/nsw_lga_suburb.csv'
TABLE_NAME = 'raw_schema.NSW_LGA_SUBURB'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'load_nsw_lga_suburb_to_db',
    default_args=default_args,
    description='A DAG to load data from GCP bucket to a Postgres DB',
    schedule_interval=None,
)

def download_data_from_gcp_bucket(**kwargs):
    """Download data from GCP bucket."""
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(OBJECT_NAME)
    blob.download_to_filename(LOCAL_FILE)
    print(f"{OBJECT_NAME} downloaded to {LOCAL_FILE}.")

def load_data_to_postgres(**kwargs):
    """Load data from CSV to Postgres."""
    # Read data from CSV
    data = pd.read_csv(LOCAL_FILE)
    tuples = [tuple(x) for x in data.to_numpy()]

    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Clear the existing table content
    cursor.execute(f"TRUNCATE TABLE {TABLE_NAME};")

    # Insert data into Postgres
    args_str = ','.join(cursor.mogrify("(%s,%s)", x).decode("utf-8") for x in tuples)
    cursor.execute(f"INSERT INTO {TABLE_NAME} (LGA_NAME, SUBURB_NAME) VALUES " + args_str)
    conn.commit()

    cursor.close()
    conn.close()

    print(f"Inserted {len(tuples)} rows into {TABLE_NAME}.")

download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data_from_gcp_bucket,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

download_task >> load_task  # Set task dependency
