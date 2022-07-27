from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

def upload_to_bucket(bucket_name, folder_name, file_name, src_path, **kwargs):
    hook = GoogleCloudStorageHook()
    hook.upload(bucket_name,
                object_name = '{}/{}'.format(folder_name, file_name),
                filename = src_path)