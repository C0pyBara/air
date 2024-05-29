from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('create_weather_data_table', default_args=default_args, schedule_interval=None)

def create_table():
    logging.info("Starting the task to create table")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    
    logging.info("Connected to the database")
    
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP,
        temperature FLOAT,
        humidity FLOAT
    );
    '''
    
    logging.info("Executing create table query: %s", create_table_query)
    cursor.execute(create_table_query)
    pg_conn.commit()
    
    logging.info("Table created successfully")
    
    cursor.close()
    pg_conn.close()
    logging.info("Database connection closed")

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)
