import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import numpy as np

# Constants
BUCKET_NAME = 'australia-southeast1-bdeenv-29f99fa6-bucket'
OBJECT_NAMES = [
    'data/05_2020.csv', 'data/06_2020.csv', 'data/07_2020.csv', 
    'data/08_2020.csv', 'data/09_2020.csv', 'data/10_2020.csv', 
    'data/11_2020.csv', 'data/12_2020.csv', 'data/01_2021.csv', 
    'data/02_2021.csv', 'data/03_2021.csv', 'data/04_2021.csv'
]
LOCAL_FILES = [
    '/tmp/05_2020.csv', '/tmp/06_2020.csv', '/tmp/07_2020.csv', 
    '/tmp/08_2020.csv', '/tmp/09_2020.csv', '/tmp/10_2020.csv', 
    '/tmp/11_2020.csv', '/tmp/12_2020.csv', '/tmp/01_2021.csv', 
    '/tmp/02_2021.csv', '/tmp/03_2021.csv', '/tmp/04_2021.csv'
]
TABLE_NAME = 'raw_schema.airbnb_listing_all'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'load_listing_all_to_db',
    default_args=default_args,
    description='A DAG to load Airbnb listing data from GCP bucket to a Postgres DB',
    schedule_interval=None,
)

@task(dag=dag)
def download_data_from_gcp_bucket(object_name, local_file):
    """Download data from GCP bucket."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)
    blob.download_to_filename(local_file)
    return local_file

@task(dag=dag)
def load_data_to_postgres(file_path):
    """Load data from CSV to Postgres."""
    data = pd.read_csv(file_path, parse_dates=['SCRAPED_DATE', 'HOST_SINCE'])
    data.replace({'NaT': None, np.nan: None}, inplace=True)
    selected_columns = [
        'LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME', 'HOST_SINCE',
        'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD', 'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE', 
        'ROOM_TYPE', 'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30', 
        'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY', 
        'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION', 
        'REVIEW_SCORES_VALUE'
    ]
    data = data[selected_columns]
    tuples = [tuple(x) for x in data.to_numpy()]
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    records_list_template = ','.join(['%s'] * data.shape[1])
    insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(selected_columns)}) VALUES ({records_list_template})"
    cursor.executemany(insert_query, tuples)
    conn.commit()
    cursor.close()
    conn.close()
    return f"Inserted {len(tuples)} rows into {TABLE_NAME}."

# Define task sequence for each file
previous_load_task = None
for obj, local in zip(OBJECT_NAMES, LOCAL_FILES):
    file_name = obj.split('/')[-1].split('.')[0]  # Extracting the file name from the path
    file_path = download_data_from_gcp_bucket(obj, local)
    load_task = load_data_to_postgres(file_path)
    
    # Ensure tasks run sequentially
    if previous_load_task:
        previous_load_task >> load_task
    previous_load_task = load_task











import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Constants
BUCKET_NAME = 'australia-southeast1-bdeenv-29f99fa6-bucket'
OBJECT_NAME = 'data/2016Census_G01_NSW_LGA.csv'  # Adjusted object name
LOCAL_FILE = '/tmp/2016Census_G01_NSW_LGA.csv'  # Adjusted local file name
TABLE_NAME = 'raw_schema.Census_G01_NSW_LGA_2016'  # Adjusted table name

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'load_census_G01_data_to_db',  # Updated DAG name
    default_args=default_args,
    description='A DAG to load Census G01 data from GCP bucket to a Postgres DB',
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
    # Since we have a large number of columns, we can use a simpler approach without specifying all column names
    placeholders = ', '.join(['%s'] * len(data.columns))  # It creates a string of %s, %s, %s,... depending on the number of columns
    table_columns = ', '.join(data.columns)  # Creates a string of the column names separated by commas
    insert_query = f"INSERT INTO {TABLE_NAME} ({table_columns}) VALUES ({placeholders})"

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













import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Constants
BUCKET_NAME = 'australia-southeast1-bdeenv-29f99fa6-bucket'
OBJECT_NAME = 'data/NSW_LGA_CODE.csv'  # Change this to the new object name
LOCAL_FILE = '/tmp/nsw_lga_code.csv'  # Change this to a new local file
TABLE_NAME = 'raw_schema.NSW_LGA_CODE'  # Change this to the new table name

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'load_nsw_lga_code_to_db',  # Change the DAG name
    default_args=default_args,
    description='A DAG to load LGA code data from GCP bucket to a Postgres DB',
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

    # Prepare the data and insert it into Postgres
    args_str = ','.join(cursor.mogrify("(%s,%s)", x).decode("utf-8") for x in tuples)
    cursor.execute(f"INSERT INTO {TABLE_NAME} (LGA_CODE, LGA_NAME) VALUES " + args_str)
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
