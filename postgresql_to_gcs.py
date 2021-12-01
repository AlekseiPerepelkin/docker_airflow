import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime

'''PostgreSQL >> load to Google Cloud Storage''' 

#default arguments 
default_args = {
    'owner': 'aleksei',
    'depends_on_past': False,    
    'start_date': datetime(2021, 11, 1),
}

#DAG
dag = DAG('postgres_to_gcs',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


# Change these to your identifiers, if needed.
GOOGLE_CONN_ID = "google_cloud_default"
POSTGRES_CONN_ID = "postgres_1"
FILENAME = "materialized_view.parquet"
SQL_QUERY = "select * from materialized_view"
bucket_name = "default"

upload_data = PostgresToGCSOperator(
        task_id="get_data", sql=SQL_QUERY, bucket=bucket_name, filename=FILENAME, gzip=False, dag=dag)
        
upload_data_server_side_cursor = PostgresToGCSOperator(
        task_id="get_data_with_server_side_cursor",
        sql=SQL_QUERY,
        bucket=bucket_name,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True,
        export_format='parquet',
        dag=dag)

upload_data >> upload_data_server_side_cursor