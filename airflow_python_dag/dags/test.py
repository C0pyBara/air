from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='Test connection to Postgres',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

test_connection = PostgresOperator(
    task_id='test_connection',
    postgres_conn_id='airflow_postgres2',
    sql='SELECT 1;',
    dag=dag,
)
