from airflow.hooks.base import BaseHook
import requests 



class TfLHook(BaseHook):
    def __init__(self, tfl_conn_id='tfl_default'):
        super().__init__()
        self.tfl_conn_id = tfl_conn_id

    def get_data(self, endpoint, params=None):
        # Lấy Key từ Airflow Variable (Admin -> Variables trên UI)
        from airflow.sdk import Variable
        api_key = Variable.get("tfl_app_key") 
        
        base_url = f"https://api.tfl.gov.uk/{endpoint}"
        payload = params or {}
        payload['app_key'] = api_key
        
        response = requests.get(base_url, params=payload)
        response.raise_for_status()
        return response.json() # Trả về Root (List hoặc Dict)