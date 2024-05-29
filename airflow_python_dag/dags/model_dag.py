from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.impute import SimpleImputer
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
import pickle
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('train_lstm_model', default_args=default_args, schedule_interval='@daily')

def train_model():
    try:
        # Путь для сохранения модели
        model_dir = '/path/to/save'
        model_path = os.path.join(model_dir, 'lstm_model.h5')
        
        # Проверка существования модели
        if os.path.exists(model_path):
            print("Model already exists. Skipping training.")
            return
        
        # Убедитесь, что путь для сохранения существует
        os.makedirs(model_dir, exist_ok=True)

        # Подключение к базе данных PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='airflow_postgres2')
        pg_conn = pg_hook.get_conn()
        query = "SELECT * FROM irkutsk"
        df = pd.read_sql(query, pg_conn)
        pg_conn.close()

        if df.empty:
            raise ValueError("No data found in the 'irkutsk' table.")
        
        # Предобработка данных
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if df['date'].isnull().any():
            raise ValueError("Some date values could not be converted.")
        df['time_unix'] = df['date'].view('int64') // 10**9
        columns_to_exclude = ['date']
        X = df.drop(columns=columns_to_exclude).values

        scaler = MinMaxScaler()
        imputer = SimpleImputer(strategy='median')
        X_imputed = imputer.fit_transform(X)
        X_scaled = scaler.fit_transform(X_imputed)
        X_scaled = X_scaled.reshape((X_scaled.shape[0], X_scaled.shape[1], 1))

        # Сохранение объектов scaler и imputer для будущего использования
        with open(os.path.join(model_dir, 'scaler.pkl'), 'wb') as f:
            pickle.dump(scaler, f)
        with open(os.path.join(model_dir, 'imputer.pkl'), 'wb') as f:
            pickle.dump(imputer, f)

        # Определение модели
        model = Sequential()
        model.add(LSTM(50, return_sequences=True, input_shape=(X_scaled.shape[1], 1)))
        model.add(LSTM(50, return_sequences=False))
        model.add(Dense(1))

        model.compile(optimizer='adam', loss='mean_squared_error')

        # Обучение модели
        model.fit(X_scaled, df['temperature'].values, epochs=10, batch_size=32)

        # Сохранение модели
        model.save(model_path)

        print("Model has been successfully trained and saved")

    except Exception as e:
        print(f"An error occurred: {e}")

train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag,
)
