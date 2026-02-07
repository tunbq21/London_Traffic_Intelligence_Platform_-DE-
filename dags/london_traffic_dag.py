from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.tfl_hook import TfLHook
from datetime import datetime, timedelta
import json
import logging
import pandas as pd

def extract_bus_data():
    try:
        hook = TfLHook()

        data = hook.get_data("Line/159/Arrivals")   
        
        file_path = f"/usr/local/airflow/include/json/bus_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        # with open(file_path, 'w') as f:
        #     json.dump(data[:100], f) 
        chunk_size = 100
        a = 0
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            df = pd.DataFrame(chunk)
            df.to_json(file_path, orient='records', lines=True, mode='a')
            a += 1
            logging.info(f"Đã trích xuất dữ liệu thành công vào {file_path} lần {a}")
        # chunks = pd.read_json(json.dumps(data), lines=False, chunksize=chunk_size)
    except Exception as e:
        logging.error(f"Lỗi trong quá trình trích xuất dữ liệu: {e}")
        raise
    

default_args = {
    'owner': 'gemini_user',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='london_bus_extraction_v1',
    default_args=default_args,
    schedule='@hourly', 
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_bus_data
    )