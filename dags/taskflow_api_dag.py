from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from hooks.tfl_hook import TfLHook
from datetime import datetime, timedelta
import json, logging, pandas as pd, csv, os
from pathlib import Path

# Cấu hình chung
COLUMNS = ["id", "category", "severity", "location", "comments", "startDateTime", "endDateTime", "lastModDateTime", "point"]

default_args = {
    'owner': 'Tuan Quang',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='london_road_disruption_taskflow_v2',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['london_traffic', 'taskflow']
)
def london_road_pipeline():

    # --- NHÓM TASK CREATE TABLE ---
    @task
    def create_local_table():
        pg_hook = PostgresHook(postgres_conn_id='local_postgres')
        pg_hook.run("DROP TABLE IF EXISTS london_road_disruptions;")
        create_sql = """
        CREATE TABLE london_road_disruptions (
            id VARCHAR(100) PRIMARY KEY, category VARCHAR(100), severity VARCHAR(100),
            location TEXT, comments TEXT, startDateTime TEXT, endDateTime TEXT,
            lastModDateTime TEXT, point TEXT, extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ); """
        pg_hook.run(create_sql)

    @task
    def create_azure_table():
        pg_hook = PostgresHook(postgres_conn_id='azure_postgres_conn')
        create_sql = """
        CREATE TABLE IF NOT EXISTS london_road_disruptions (
            id VARCHAR(100) PRIMARY KEY, category VARCHAR(100), severity VARCHAR(100),
            location TEXT, comments TEXT, startDateTime TEXT, endDateTime TEXT,
            lastModDateTime TEXT, point TEXT, extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ); """
        pg_hook.run(create_sql)

    # --- NHÓM EXTRACTION & TRANSFORMATION ---
    with TaskGroup(group_id='extraction_and_prep') as extraction_prep:
        
        @task
        def extract_data():
            hook = TfLHook()
            data = hook.get_data("Road/All/Disruption")
            if not data: return None

            folder_path = "/usr/local/airflow/include/json/road_disruption"
            Path(folder_path).mkdir(parents=True, exist_ok=True)
            file_path = f"{folder_path}/data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            
            df = pd.json_normalize(data)
            for col in COLUMNS:
                if col not in df.columns: df[col] = None
            
            df_filtered = df[COLUMNS].copy()
            df_filtered['point'] = df_filtered['point'].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
            df_filtered.to_json(file_path, orient='records', lines=True)
            return file_path

        @task
        def convert_to_csv(jsonl_path: str):
            if not jsonl_path: return None
            df = pd.read_json(jsonl_path, lines=True)
            csv_path = jsonl_path.replace('.jsonl', '.csv').replace('/json/', '/csv/')
            Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL, na_rep='')
            return csv_path

        # Thiết lập luồng trong TaskGroup
        jsonl_file = extract_data()
        csv_file = convert_to_csv(jsonl_file)

    # --- NHÓM LOAD DATA ---
    @task
    def load_local(csv_path: str):
        if not csv_path: return
        pg_hook = PostgresHook(postgres_conn_id='local_postgres')
        copy_sql = f"""
            COPY london_road_disruptions({','.join(COLUMNS)})
            FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL '');
        """
        pg_hook.copy_expert(sql=copy_sql, filename=csv_path)

    @task
    def load_azure(csv_path: str):
        if not csv_path: return
        pg_hook = PostgresHook(postgres_conn_id='azure_postgres_conn')
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("CREATE TEMP TABLE temp_london_road (LIKE london_road_disruptions INCLUDING ALL);")
            with open(csv_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(sql=f"COPY temp_london_road({','.join(COLUMNS)}) FROM STDIN WITH (FORMAT CSV, HEADER, NULL '');", file=f)
            
            upsert_sql = """
                INSERT INTO london_road_disruptions SELECT * FROM temp_london_road
                ON CONFLICT (id) DO UPDATE SET 
                    category = EXCLUDED.category, severity = EXCLUDED.severity,
                    location = EXCLUDED.location, comments = EXCLUDED.comments,
                    extracted_at = CURRENT_TIMESTAMP;
            """
            cur.execute(upsert_sql)
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # 1. Chạy song song 2 task tạo bảng
    setup_tables = [create_local_table(), create_azure_table()]
    
    # 2. Chạy extraction (csv_file là kết quả cuối cùng của TaskGroup)
    
    # Thiết lập phụ thuộc
    setup_tables >> extraction_prep
    
    # 3. Load dữ liệu ra 2 nơi từ file CSV thu được
    load_local(csv_file)
    load_azure(csv_file)

# Khởi tạo DAG
london_road_pipeline()