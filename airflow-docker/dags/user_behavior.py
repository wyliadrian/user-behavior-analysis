from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from daglibs import gcp_connection, local_to_gcp_bucket, bigquery_job

#Config
BUCKET_NAME = Variable.get("BUCKET")

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_dwonstream": True,
    "start_date": datetime(2022, 7, 27, 5, 3),
    "email": ["wyliadrian18@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes = 1)
}

dag = DAG(
    "user_behavior",
    default_args = default_args,
    schedule_interval = "*/2 * * * *",
    max_active_runs = 1
)

activate_google_cloud = PythonOperator(
    dag = dag,
    task_id = "add_gcp_connection",
    python_callable = gcp_connection.add_gcp_connection,
    provide_context = True
)

extract_user_purchase_data = PostgresOperator(
    dag = dag,
    task_id = "extract_user_purchase_data",
    sql = "./daglibs/sql/extract_user_purchase.sql",
    postgres_conn_id = "postgres_default"
)

user_purchase_data_to_gcs_stage = PythonOperator(
    dag = dag,
    task_id = "user_purchase_data_to_gcs_stage",
    python_callable = local_to_gcp_bucket.upload_to_bucket,
    provide_context = True,
    op_kwargs = {
        "bucket_name": BUCKET_NAME, 
        "folder_name": "stage/user_purchase", 
        "file_name": "{{  execution_date | ds }}.csv", 
        "src_path": "/opt/airflow/temp/{{  execution_date | ds }}.csv"
    }
)

sql_query = "LOAD DATA INTO retail_stage.user_purchase PARTITION BY TIMESTAMP_TRUNC(insertion_date, DAY) FROM FILES (format = 'CSV',skip_leading_rows = 1, uris = ['gs://"+BUCKET_NAME+"/stage/user_purchase/{{  execution_date | ds }}.csv'])"

load_user_purchase_data_to_bq_tbl = PythonOperator(
    dag = dag,
    task_id = "load_user_purchase_data_to_bq_tbl",
    python_callable = bigquery_job.run_bigquery_external_query,
    op_kwargs = {
        "query": sql_query
    }
)

remove_temp_user_purchase_data = BashOperator(
    dag = dag,
    task_id = "remove_temp_user_purchase_data",
    bash_command = "rm /opt/airflow/temp/{{  execution_date | ds }}.csv"
)

(
    activate_google_cloud
    >> extract_user_purchase_data
    >> user_purchase_data_to_gcs_stage
    >> load_user_purchase_data_to_bq_tbl
)

(
    user_purchase_data_to_gcs_stage
    >> remove_temp_user_purchase_data
)