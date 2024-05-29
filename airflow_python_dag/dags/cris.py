from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import os
import json
import re
import pandas as pd
from matplotlib import pyplot as plt
from tensorflow.keras.models import load_model
from sklearn.impute import SimpleImputer
import pickle

# Дефолтные аргументы для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('fetch_parse_store_report_weather_data', default_args=default_args, schedule_interval=timedelta(hours=1))

# Функция для загрузки и сохранения данных о погоде
def fetch_and_save_weather_data():
    url = "http://geos.icc.ru:8080/weather"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.text
        parsed_data = parse_weather_data(data)
        output_file_path = os.path.join('/tmp', 'output.json')
        
        # Проверка, существует ли файл, и добавление новых данных к существующим
        if os.path.exists(output_file_path):
            with open(output_file_path, 'r') as f:
                existing_data = json.load(f)
            existing_data.append(parsed_data)
            data_to_save = existing_data
        else:
            data_to_save = [parsed_data]
        
        with open(output_file_path, 'w') as f:
            json.dump(data_to_save, f)
        print("Data has been successfully saved to the JSON file")
    else:
        print("Failed to fetch data from the URL")

# Функция для парсинга данных о погоде
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

# Функция для сохранения данных в базу данных
def save_data_to_db():
    # Чтение данных из JSON файла
    input_file_path = os.path.join('/tmp', 'output.json')
    try:
        with open(input_file_path, 'r') as f:
            data_list = json.load(f)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return
    
    # Подключение к базе данных PostgreSQL
    try:
        pg_hook = PostgresHook(postgres_conn_id='airflow_postgres2')
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return

    try:
        # Создание таблицы, если она не существует
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            date TIMESTAMP,
            temperature FLOAT,
            pressure FLOAT
        );
        """)
        pg_conn.commit()

        # Вставка данных
        for data in data_list:
            cursor.execute(
                "INSERT INTO weather_data (date, temperature, pressure) VALUES (%s, %s, %s)",
                (data['date'], data['temperature'], data['pressure'])
            )
        pg_conn.commit()
        print("Data has been successfully saved to the database")
    except Exception as e:
        pg_conn.rollback()
        print(f"Error saving data to the database: {e}")
    finally:
        cursor.close()
        pg_conn.close()

# Функция для генерации и сохранения отчета с графиками
def generate_report():
    try:
        # Подключение к базе данных PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='airflow_postgres2')
        pg_conn = pg_hook.get_conn()
        query = "SELECT * FROM weather_data"
        df = pd.read_sql(query, pg_conn)
    except Exception as e:
        print(f"Error connecting to the database or fetching data: {e}")
        return

    try:
        # Генерация отчета
        report = df.describe().to_string()
        report_file_path = os.path.join('/tmp', 'report.txt')
        with open(report_file_path, 'w') as f:
            f.write(report)
    except Exception as e:
        print(f"Error generating or saving the report: {e}")
        return

    try:
        # Генерация графиков
        plt.figure(figsize=(10, 5))
        plt.plot(df['date'], df['temperature'], marker='o', linestyle='-', color='b', label='Temperature')
        plt.xlabel('Date')
        plt.ylabel('Temperature (C)')
        plt.title('Temperature Over Time')
        plt.legend()
        temperature_plot_path = os.path.join('/tmp', 'temperature_plot.png')
        plt.savefig(temperature_plot_path)
        plt.close()
    except Exception as e:
        print(f"Error generating temperature plot: {e}")
        return
    
    try:
        plt.figure(figsize=(10, 5))
        plt.plot(df['date'], df['pressure'], marker='o', linestyle='-', color='r', label='Pressure')
        plt.xlabel('Date')
        plt.ylabel('Pressure (mm Hg)')
        plt.title('Pressure Over Time')
        plt.legend()
        pressure_plot_path = os.path.join('/tmp', 'pressure_plot.png')
        plt.savefig(pressure_plot_path)
        plt.close()
    except Exception as e:
        print(f"Error generating pressure plot: {e}")
        return

    print("Report and graphs have been successfully generated")

# Функция для отправки отчета в Telegram
def send_report_to_telegram():
    bot_token = '7141646480:AAETrVWTjNZxnaRiD10AXUKmU1rV0w6Ergg'
    chat_id = '534239907'
    files = ['/tmp/report.txt', '/tmp/temperature_plot.png', '/tmp/pressure_plot.png']
    message = 'Please find attached the latest weather data report along with graphs.'

    for file in files:
        if not os.path.exists(file):
            print(f"File {file} does not exist")
            continue

        with open(file, 'rb') as f:
            response = requests.post(
                f'https://api.telegram.org/bot{bot_token}/sendDocument',
                data={'chat_id': chat_id, 'caption': message},
                files={'document': f}
            )
            if response.status_code != 200:
                print(f"Failed to send {file} to Telegram: {response.text}")
            else:
                print(f"Successfully sent {file} to Telegram")

# Определение задач в DAG
fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_weather_data',
    python_callable=fetch_and_save_weather_data,
    dag=dag,
)

save_data_task = PythonOperator(
    task_id='save_data_to_db',
    python_callable=save_data_to_db,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

send_telegram_task = PythonOperator(
    task_id='send_report_to_telegram',
    python_callable=send_report_to_telegram,
    dag=dag,
)

# Устанавливаем зависимости между задачами
fetch_and_save_task >> save_data_task >> generate_report_task >> send_telegram_task
