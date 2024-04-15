from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('fetch_and_parse_weather_data', default_args=default_args, schedule_interval=timedelta(hours=1))

def fetch_and_save_weather_data():
    url = "http://geos.icc.ru:8080/weather"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.text
        parsed_data = parse_weather_data(data)
        output_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output.json')
        with open(output_file_path, 'w') as f:
            json.dump(parsed_data, f)
        print("Data has been successfully saved to the JSON file")
    else:
        print("Failed to fetch data from the URL")

def parse_weather_data(data):
    match = re.search(r'Data (\d{2}-\d{2}-\d{4} \d{2}:\d{2}) Temperature ([\d.-]+) C\. Pressure ([\d.-]+) mm\. hg\.', data)
    if match:
        return {
            'date': match.group(1),
            'temperature': float(match.group(2)),
            'pressure': float(match.group(3))
        }
    else:
        print("Failed to parse weather data")
        return {}

fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_weather_data',
    python_callable=fetch_and_save_weather_data,
    dag=dag,
)
