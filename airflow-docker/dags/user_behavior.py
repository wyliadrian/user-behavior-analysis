from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from daglibs import gcp_connection, local_to_gcp_bucket, bigquery_job

#Config
BUCKET_NAME = Variable.get("BUCKET")
CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_dwonstream": True,
    "start_date": datetime(2022, 7, 30, 19, 48),
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

load_user_purchase_data_to_bq = BigQueryExecuteQueryOperator(
    dag = dag,
    task_id = "load_user_purchase_data_to_bq",
    sql = './daglibs/sql/load_user_purchase_data.sql',
    use_legacy_sql = False
)

remove_temp_user_purchase_data = BashOperator(
    dag = dag,
    task_id = "remove_temp_user_purchase_data",
    bash_command = "rm /opt/airflow/temp/{{  execution_date | ds }}.csv"
)

movie_review_to_gcs_raw = PythonOperator(
    dag = dag,
    task_id = "movie_review_to_gcs_raw",
    python_callable = local_to_gcp_bucket.local_to_bucket,
    provide_context = True,
    op_kwargs = {
        "bucket_name": BUCKET_NAME, 
        "source_file_name": "/opt/airflow/data/movie_review.csv", 
        "destination_blob_name": f"raw/movie_review.csv"
    }
)

"""
movie_review_to_gcs_raw = PythonOperator(
    dag = dag,
    task_id = "movie_review_to_gcs_raw",
    python_callable = local_to_gcp_bucket.upload_to_bucket,
    provide_context = True,
    op_kwargs = {
        "bucket_name": BUCKET_NAME, 
        "folder_name": "raw", 
        "file_name": "movie_review.csv", 
        "src_path": "/opt/airflow/data/movie_review.csv"
    }
)
"""

spark_script_to_gcs_raw = PythonOperator(
    dag = dag,
    task_id = "spark_script_to_gcs_raw",
    python_callable = local_to_gcp_bucket.upload_to_bucket,
    provide_context = True,
    op_kwargs = {
        "bucket_name": BUCKET_NAME, 
        "folder_name": "scripts", 
        "file_name": "random_text_classification.py", 
        "src_path": "/opt/airflow/dags/daglibs/random_text_classification.py"
    }
)

src_path = f"gs://{BUCKET_NAME}/raw/movie_review.csv"
dst_path = "gs://uba-bkt/stage/movie_review/{{  execution_date | ds }}.parquet"

movie_review_classification = DataProcPySparkOperator(
    dag = dag,
    task_id = "movie_review_classification",
    main = f"gs://{BUCKET_NAME}/scripts/random_text_classification.py",
    cluster_name = CLUSTER_NAME,
    region = "us-central1",
    arguments = [src_path, dst_path]
)

load_movie_review_classification_to_bq = BigQueryExecuteQueryOperator(
    dag = dag,
    task_id = "load_movie_review_classification_to_bq",
    sql = "./daglibs/sql/load_classified_movie_review.sql",
    use_legacy_sql = False
)

generate_user_behavior_metric = BigQueryExecuteQueryOperator(
    dag = dag,
    task_id = "generate_user_behavior_metric",
    sql = "./daglibs/sql/generate_user_behavior_metric.sql",
    use_legacy_sql = False
)

end_of_data_pipeline = DummyOperator(
    task_id = "end_of_data_pipeline", 
    dag = dag
)

(
    [
        activate_google_cloud,
        extract_user_purchase_data
    ]
    >> user_purchase_data_to_gcs_stage
    >> [
        load_user_purchase_data_to_bq,
        remove_temp_user_purchase_data
        ]
)

(
    activate_google_cloud
    >> [
        movie_review_to_gcs_raw,
        spark_script_to_gcs_raw
        ]
    >> movie_review_classification
    >> load_movie_review_classification_to_bq
)

(
    [
        load_user_purchase_data_to_bq,
        load_movie_review_classification_to_bq,
    ]
    >> generate_user_behavior_metric
    >> end_of_data_pipeline
)