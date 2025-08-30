#!/usr/bin/env python3
import os
import time
from airflow.models import Variable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_airflow_variables():
    """Инициализация Airflow Variables из переменных окружения"""
    # Даем время для инициализации Airflow
    time.sleep(5)
    
    variables_to_set = {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'localhost'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '9000'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'default'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', ''),
        'SPARK_MASTER': os.environ.get('SPARK_MASTER', 'local[*]'),
    }
    
    for key, value in variables_to_set.items():
        try:
            # Проверяем, существует ли уже переменная
            existing_value = Variable.get(key, default_var=None)
            if existing_value != value:
                Variable.set(key, value)
                logger.info(f"Set variable: {key} = {value}")
            else:
                logger.info(f"Variable {key} already set to correct value")
        except Exception as e:
            logger.error(f"Failed to set variable {key}: {e}")
            # Продолжаем работу даже если не удалось установить переменные

if __name__ == "__main__":
    init_airflow_variables()