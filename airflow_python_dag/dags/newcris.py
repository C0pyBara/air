# Подключим все необходимые библиотеки и переменные среды
import requests
import json
import os

# Определим функцию для получения данных
def fetch_cris_data():
    url = "http://cris.icc.ru/dataset/list?f=3074&count_rows=true&unique=undefined&count_rows=1&iDisplayStart=0&f_fdt=2022-09-01%2000:00:00%20-%202022-10-01%2000:00:00"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.text
        return data
    else:
        print(f"Failed to fetch data from the URL: {response.status_code}")
        return None

# Определим функцию для парсинга данных
def parse_cris_data(data):
    try:
        # Предполагая, что данные представляют собой строку JSON, обернутую в теги `<pre>`
        data = json.loads(data.split("<pre>")[1].split("</pre>")[0])
        return data["aaData"]
    except (json.JSONDecodeError, IndexError) as e:
        print(f"Failed to parse CRIS data: {e}")
        return None

# Определим функцию для сохранения данных
def save_cris_data(data):
    try:
        output_file_path = 'cris_data.json'
        with open(output_file_path, 'w') as f:
            json.dump(data, f)
        print(f"Data has been successfully saved to {output_file_path}")
    except Exception as e:
        print(f"Failed to save CRIS data: {e}")

# Вызовем каждую функцию по отдельности и проверим результаты
data = fetch_cris_data()
parsed_data = parse_cris_data(data)
save_cris_data(parsed_data)
