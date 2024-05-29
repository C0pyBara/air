import sqlite3
import pandas as pd
from sqlalchemy import create_engine

# Путь к базе данных SQLite
sqlite_db_path = 'C:/Users/YtkaB/Desktop/local_database.db'

# Подключение к базе данных SQLite
sqlite_conn = sqlite3.connect(sqlite_db_path)

# Чтение данных из таблицы local_weather_data в DataFrame
df = pd.read_sql_query("SELECT * FROM local_weather_data", sqlite_conn)
sqlite_conn.close()

# Подключение к базе данных PostgreSQL
engine = create_engine('postgresql://postgres:air@localhost:5432/postgres')

# Загрузка данных в таблицу weather_data в PostgreSQL
df.to_sql('weather_data', engine, if_exists='replace', index=False)

print("Data has been successfully loaded from SQLite to PostgreSQL")
