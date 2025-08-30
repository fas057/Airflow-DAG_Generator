import os
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def watch_sources_directory(sources_dir="/opt/airflow/sources", check_interval=30):
    """Мониторинг изменений в папке sources"""
    from dag_generator import DAGGenerator, generated_dags
    
    generator = DAGGenerator(sources_dir)
    last_modified = {}
    
    while True:
        try:
            current_modified = {}
            patterns = ['*.py', '*.sql', '*.jar']
            
            # Получение времени изменения файлов
            for pattern in patterns:
                for file_path in Path(sources_dir).glob(pattern):
                    current_modified[str(file_path)] = file_path.stat().st_mtime
            
            # Проверка изменений
            if current_modified != last_modified:
                logger.info("Detected changes in sources directory. Regenerating DAGs...")
                
                # Генерация новых DAG'ов
                new_dags = generator.generate_all_dags()
                
                # Обновление глобальных переменных
                for dag in new_dags:
                    generated_dags[dag.dag_id] = dag
                    globals()[dag.dag_id] = dag
                
                last_modified = current_modified
                logger.info(f"Regenerated {len(new_dags)} DAGs")
            
            time.sleep(check_interval)
            
        except Exception as e:
            logger.error(f"Error in watch_sources_directory: {e}")
            time.sleep(check_interval)

if __name__ == "__main__":
    watch_sources_directory()