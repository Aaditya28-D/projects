import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Constants
BUCKET_NAME = 'australia-southeast1-bdeenv-29f99fa6-bucket'
OBJECT_NAME = 'data/2016Census_G02_NSW_LGA.csv'  # New object name
LOCAL_FILE = '/tmp/2016Census_G02_NSW_LGA.csv'  # New local file name
TABLE_NAME = 'raw_schema.Census_G02_NSW_LGA_2016'  # New table name

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'load_census_G02_data_to_db',  # Updated DAG name
    default_args=default_args,
    description='A DAG to load Census data from GCP bucket to a Postgres DB',
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

    # Prepare the INSERT statement
    insert_query = f"""
        INSERT INTO {TABLE_NAME} (
            LGA_CODE_2016, 
            Median_age_persons,
            Median_mortgage_repay_monthly,
            Median_tot_prsnl_inc_weekly,
            Median_rent_weekly,
            Median_tot_fam_inc_weekly,
            Average_num_psns_per_bedroom,
            Median_tot_hhd_inc_weekly,
            Average_household_size
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Insert data into Postgres
    cursor.executemany(insert_query, tuples)
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
