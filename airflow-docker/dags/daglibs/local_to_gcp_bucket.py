import os
from google.cloud import storage
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

def upload_to_bucket(bucket_name, folder_name, file_name, src_path, **kwargs):
    hook = GoogleCloudStorageHook()
    hook.upload(bucket_name,
                object_name = '{}/{}'.format(folder_name, file_name),
                filename = src_path)

def local_to_bucket(bucket_name, source_file_name, destination_blob_name):
    access_key_path = "/opt/airflow/dags/daglibs/gcp-key-file.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = access_key_path

    client = storage.Client().from_service_account_json(access_key_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    if not blob.exists():
        blob.upload_from_filename(source_file_name)