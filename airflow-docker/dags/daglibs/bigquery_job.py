from airflow.contrib.hooks.bigquery_hook import BigQueryHook

def run_bigquery_external_query(query):
    hook = BigQueryHook(use_legacy_sql = False)
    hook.run(sql = query)