# schedule_interval: @daily
# start_date: 2025-08-29
# catchup: false

def extract_data(**kwargs):
    
    import pandas as pd
    print("📥 Extracting data...")
    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'value': [100, 200, 150, 300, 250]
    })
    print(f"📊 Extracted {len(data)} rows")
    return data

def transform_data(**kwargs):
    """Трансформация данных"""
    print("🔄 Transforming data...")
    
    # Получаем данные из предыдущей задачи
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='python_task_1_extract_data')
    
    if data is None:
        print("⚠️ No data received, using sample data")
        import pandas as pd
        data = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    
    # Простая трансформация
    data['processed'] = data['value'] * 1.1 if 'value' in data.columns else data['col1'] * 2
    print(f"✅ Transformed {len(data)} rows")
    return data

def load_data(**kwargs):
    """Загрузка данных (без реального подключения)"""
    print("💾 Simulating data load...")
    
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='python_task_2_transform_data')
    
    if data is not None:
        print(f"📦 Would load {len(data)} rows to database")
        print(f"📊 Data sample: {data.head(3).to_dict()}")
    else:
        print("⚠️ No data to load")
    
    return "✅ Симуляция загрузки прошла успешно"

def main_etl(**kwargs):
    """Основная ETL функция"""
    print("🎯 ETL process started")
    
    # Можно вызывать другие функции или просто координировать
    print("✅ All ETL steps completed successfully")
    return "ETL process finished"