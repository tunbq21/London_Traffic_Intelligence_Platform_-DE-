from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.tfl_hook import TfLHook
from datetime import datetime, timedelta
import json
import logging
import pandas as pd

# def extract_bus_data():
#     try:
#         hook = TfLHook()

#         data = hook.get_data("Line/159/Arrivals")   
        
#         file_path = f"/usr/local/airflow/include/json/bus_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
#         # with open(file_path, 'w') as f:
#         #     json.dump(data[:100], f) 
#         chunk_size = 100
#         a = 0
#         for i in range(0, len(data), chunk_size):
#             chunk = data[i : i + chunk_size]
#             # df = pd.DataFrame(chunk)
#             df = pd.json_normalize(chunk,meta=['lineId','lineName',"stationName",'direction','towards','timeToStation','vehicleId','stationName'])
#             df.to_json(file_path, orient='records', lines=True, mode='a')
#             a += 1
#             logging.info(f"Đã trích xuất dữ liệu thành công vào {file_path} lần {a}")
    
#         return file_path
#         # chunks = pd.read_json(json.dumps(data), lines=False, chunksize=chunk_size)
#     except Exception as e:
#         logging.error(f"Lỗi trong quá trình trích xuất dữ liệu: {e}")
#         raise


def extract_bus_data():
    try:
        hook = TfLHook()
        data = hook.get_data("Line/159/Arrivals") 

        if not data:
            logging.info("Không có dữ liệu từ API.")
            return None

        file_path = f"/usr/local/airflow/include/json/bus_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        
        SELECTED_COLUMNS = [
            'lineId', 'lineName', 'stationName', 'direction', 
            'towards', 'timeToStation', 'vehicleId'
        ]

        chunk_size = 100
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            
            df = pd.json_normalize(chunk)
            
            # Lọc đúng những cột cần lấy (Đây mới là lúc "SELECT")
            # Dùng intersection để tránh lỗi nếu API bỗng dưng thiếu mất 1 cột
            existing_cols = df.columns.intersection(SELECTED_COLUMNS)
            df_filtered = df[existing_cols]
            
            df_filtered.to_json(file_path, orient='records', lines=True, mode='a')
            
            logging.info(f"Đã xử lý chunk {i//chunk_size + 1}, ghi vào {file_path}")

        return file_path
        
    except Exception as e:
        logging.error(f"Lỗi trích xuất: {e}")
        raise 


def jsonl_to_csv(**kwargs):
    jsonl_file = kwargs['ti'].xcom_pull(task_ids='extraction_group.extract_from_api')
    try:
        df = pd.read_json(jsonl_file, lines=True)
        file_name = jsonl_file.split('/')[-1].split('.')[0]
        csv_file = f"/usr/local/airflow/include/csv/{file_name}.csv"
        df.to_csv(csv_file, index=False)
        logging.info(f"Chuyển đổi từ JSONL sang CSV thành công: {csv_file}")
    except Exception as e:
        logging.error(f"Lỗi trong quá trình chuyển đổi JSONL sang CSV: {e}")
        raise   


default_args = {
    'owner': 'Tuan Quang',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['tbuiquang2002@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='london_bus_extraction_v1',
    default_args=default_args,
    schedule='@daily', 
    catchup=False
) as dag:
    with TaskGroup('extraction_group') as extraction_group:    
        task_extract = PythonOperator(
            task_id='extract_from_api',
            python_callable=extract_bus_data
        )
        task_transform = PythonOperator(
            task_id='jsonl_to_csv',
            python_callable=jsonl_to_csv,
            # op_kwargs={'file_path': "{{ ti.xcom_pull(task_ids='extraction_group.extract_from_api') }}"}
        )
        task_extract >> task_transform
