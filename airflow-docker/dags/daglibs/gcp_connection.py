import json

from airflow import settings
from airflow.models import Connection

def add_gcp_connection(**kwargs):
    new_conn = Connection(
        conn_id = "google_cloud_default",
        conn_type = "google_cloud_platform"
    )
    extra_field = {
        "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
        "extra__google_cloud_platform__project": "user-behavior-analysis",
        "extra__google_cloud_platform__key_path": "/opt/airflow/dags/daglibs/gcp-key-file.json"
    }

    session = settings.Session()

    # check if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        my_connection.set_extra(json.dumps(extra_field))
        session.add(my_connection)
        session.commit()
    else:
        new_conn.set_extra(json.dumps(extra_field))
        session.add(new_conn)
        session.commit()