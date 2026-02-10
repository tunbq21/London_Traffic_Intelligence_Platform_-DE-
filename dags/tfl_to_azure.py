from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_blob_exists():
    # 'azure_blob_connection' là Conn Id bạn đã tạo ở bước 3
    hook = WasbHook(wasb_conn_id='azure_blob_connection')
    container = "bronze"
    blob = "Movies_dataset.csv"
    
    if hook.check_for_blob(container, blob):
        print(f"Tìm thấy file {blob}!")
    else:
        print("Không thấy file đâu cả.")

with DAG('test_azure_connection', start_date=datetime(2024, 1, 1), schedule=None) as dag:
    task = PythonOperator(
        task_id='check_azure_file',
        python_callable=check_blob_exists
    )