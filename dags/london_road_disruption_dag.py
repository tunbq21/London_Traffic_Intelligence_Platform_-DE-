from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import TaskGroup
from hooks.tfl_hook import TfLHook
from datetime import datetime, timedelta
import json, os
import logging
import pandas as pd
import csv
from pathlib import Path
import psycopg2

# Danh sách cột cố định
COLUMNS = [
    "id", "category", "severity", "location", "comments", "startDateTime", 
    "endDateTime", "lastModDateTime", "point"
]

def extract_road_disruption_data():
    try: 
        hook = TfLHook()
        data = hook.get_data("Road/All/Disruption")
        if not data:
            logging.info("Không có dữ liệu từ API.")
            return None

        folder_path = "/usr/local/airflow/include/json/road_disruption"
        Path(folder_path).mkdir(parents=True, exist_ok=True)
        file_path = f"{folder_path}/road_disruption_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        
        chunk_size = 200
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            df = pd.json_normalize(chunk)
            for col in COLUMNS:
                if col not in df.columns:
                    df[col] = None
            
            df_filtered = df[COLUMNS].copy()
            # Xử lý tọa độ point thành chuỗi JSON
            df_filtered['point'] = df_filtered['point'].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
            )
            df_filtered.to_json(file_path, orient='records', lines=True, mode='a')
            
        return file_path
    except Exception as e:
        logging.error(f"Lỗi extract: {e}")
        raise

def jsonl_to_csv(**kwargs):
    try:
        ti = kwargs['ti']
        jsonl_file_path = ti.xcom_pull(task_ids='extraction_group.extract_road_disruption_data')
        if not jsonl_file_path: return None
            
        df = pd.read_json(jsonl_file_path, lines=True)
        df = df[COLUMNS]
        
        csv_file_path = jsonl_file_path.replace('.jsonl', '.csv').replace('/json/', '/csv/')
        Path(csv_file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Lưu CSV với các ô trống thực sự để Postgres dễ nhận diện NULL
        df.to_csv(csv_file_path, index=False, quoting=csv.QUOTE_MINIMAL, na_rep='')
        return csv_file_path
    except Exception as e:
        logging.error(f"Lỗi convert CSV: {e}")
        raise

def create_road_table():
    pg_hook = PostgresHook(postgres_conn_id='local_postgres')
    # DROP bảng cũ để đồng bộ kiểu dữ liệu mới
    pg_hook.run("DROP TABLE IF EXISTS london_road_disruptions;")
    
    create_sql = """
    CREATE TABLE london_road_disruptions (
        id VARCHAR(100) PRIMARY KEY,
        category VARCHAR(100),
        severity VARCHAR(100),
        location TEXT,
        comments TEXT,
        startDateTime TEXT, -- Chuyển sang TEXT để tránh lỗi định dạng khi load
        endDateTime TEXT,
        lastModDateTime TEXT,
        point TEXT,
        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_hook.run(create_sql)

def load_csv_to_postgres(**kwargs):
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='extraction_group.jsonl_to_csv')
    if not csv_file_path: return

    pg_hook = PostgresHook(postgres_conn_id='local_postgres')
    
    # Sử dụng lệnh COPY với tùy chọn NULL AS ''
    copy_sql = """
        COPY london_road_disruptions(id, category, severity, location, comments, startDateTime, endDateTime, lastModDateTime, point)
        FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL '');
    """
    pg_hook.copy_expert(sql=copy_sql, filename=csv_file_path)




def create_road_table_azure():
    try:
        # Lấy link từ biến môi trường
        conn_uri = os.getenv('AIRFLOW_CONN_AZURE_POSTGRES_CONN')
        conn = psycopg2.connect(conn_uri)
        
        cur = conn.cursor()
        create_sql = """
        DROP TABLE IF EXISTS london_road_disruptions;
        CREATE TABLE london_road_disruptions (
            id VARCHAR(100) PRIMARY KEY,
            category VARCHAR(100),
            severity VARCHAR(100),
            location TEXT,
            comments TEXT,
            startDateTime TEXT,
            endDateTime TEXT,
            lastModDateTime TEXT,
            point TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_sql)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Kết nối và tạo bảng Azure thành công bằng psycopg2 + ENV!")
    except Exception as e:
        logging.error(f"Lỗi: {e}")
        raise


def load_csv_to_azure_postgres(**kwargs):
    ti = kwargs['ti']
    # Lấy đường dẫn file CSV từ task trước đó trong group
    csv_file_path = ti.xcom_pull(task_ids='extraction_group.jsonl_to_csv')
    
    if not csv_file_path:
        logging.warning("Không tìm thấy đường dẫn file CSV từ XCom.")
        return

    try:
        conn_uri = os.getenv('AIRFLOW_CONN_AZURE_POSTGRES_CONN')
        conn = psycopg2.connect(conn_uri)
        
        cur = conn.cursor()
        
        # Mở file CSV và sử dụng lệnh copy_expert (hoặc copy_from)
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            copy_sql = """
                COPY london_road_disruptions(id, category, severity, location, comments, startDateTime, endDateTime, lastModDateTime, point)
                FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL '');
            """
            cur.copy_expert(sql=copy_sql, file=f)
            
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Đã load dữ liệu thành công từ {csv_file_path} lên Azure Postgres.")
    except Exception as e:
        logging.error(f"Lỗi khi load dữ liệu lên Azure: {e}")
        raise



default_args = {
    'owner': 'Tuan Quang',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='london_road_disruption_dag',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    create_table_task = PythonOperator(
        task_id='create_road_table',
        python_callable=create_road_table
    )

    with TaskGroup('extraction_group') as extraction_group:
        extract_task = PythonOperator(
            task_id='extract_road_disruption_data',
            python_callable=extract_road_disruption_data
        )
        jsonl_to_csv_task = PythonOperator(
            task_id='jsonl_to_csv',
            python_callable=jsonl_to_csv,
        )
        extract_task >> jsonl_to_csv_task

    load_data_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres,
    )

    create_table_filess_task = PythonOperator(
        task_id='create_road_table_azure',
        python_callable=create_road_table_azure,
    )

    # Task load data lên Filess
    load_data_filess_task = PythonOperator(
        task_id='load_csv_to_azure_postgres',
        python_callable=load_csv_to_azure_postgres,
    )


    [create_table_task, create_table_filess_task] >> extraction_group 

    extraction_group >> [load_data_task, load_data_filess_task]