from datetime import datetime, timedelta
from airflow import DAG, macros
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_dwonstream": True,
    "start_date": datetime(2022, 7, 25, 5, 20),
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

extract_user_purchase_data = PostgresOperator(
    dag = dag,
    task_id = "extract_user_purchase_data",
    sql = "./daglibs/sql/extract_user_purchase.sql",
    postgres_conn_id = "postgres_default"
)

load_user_purchase_data = PostgresOperator(
    dag = dag,
    task_id = "load_user_purchase_data",
    sql = "./daglibs/sql/load_user_purchase.sql",
    postgres_conn_id = "postgres_default"
)

(
    extract_user_purchase_data
    >> load_user_purchase_data
)