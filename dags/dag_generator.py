import os
import re
import ast
import glob
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

class DAGGenerator:
    def __init__(self, sources_dir="/opt/airflow/sources"):
        self.sources_dir = sources_dir
        self.connection_params = self._get_connection_params()
    
    def _get_connection_params(self):
        """Получение параметров подключения из переменных окружения или Airflow Variables"""
        params = {}
        
        # Пробуем получить из переменных окружения
        params['clickhouse_host'] = os.environ.get('CLICKHOUSE_HOST', 'localhost')
        params['clickhouse_port'] = os.environ.get('CLICKHOUSE_PORT', '9000')
        params['clickhouse_user'] = os.environ.get('CLICKHOUSE_USER', 'default')
        params['clickhouse_password'] = os.environ.get('CLICKHOUSE_PASSWORD', '')
        params['spark_master'] = os.environ.get('SPARK_MASTER', 'local[*]')
        
        # Если в переменных окружения нет, пробуем Airflow Variables
        try:
            if not all([params['clickhouse_host'], params['clickhouse_port']]):
                params['clickhouse_host'] = Variable.get("CLICKHOUSE_HOST", default_var="localhost")
                params['clickhouse_port'] = Variable.get("CLICKHOUSE_PORT", default_var="9000")
                params['clickhouse_user'] = Variable.get("CLICKHOUSE_USER", default_var="default")
                params['clickhouse_password'] = Variable.get("CLICKHOUSE_PASSWORD", default_var="")
                params['spark_master'] = Variable.get("SPARK_MASTER", default_var="local[*]")
        except:
            logger.warning("Airflow Variables not available, using environment variables")
        
        return params
    
    def _parse_python_file(self, file_path):
        """Парсинг Python файла для извлечения функций"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        try:
            tree = ast.parse(content)
            functions = []
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    functions.append({
                        'name': node.name,
                        'lineno': node.lineno,
                    })
            
            return functions
        except SyntaxError as e:
            logger.warning(f"Syntax error in {file_path}: {e}")
            return []
    
    def _parse_sql_file(self, file_path):
        """Парсинг SQL файла для извлечения DDL команд"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Разделение на отдельные команды по точкам с запятой
        commands = []
        current_command = ""
        
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('--') or not line:
                continue
            
            current_command += line + " "
            
            if ';' in line:
                commands.append(current_command.strip())
                current_command = ""
        
        if current_command.strip():
            commands.append(current_command.strip())
        
        return commands
    
    def _create_python_task(self, dag, file_path, function_name, task_id):
        """Создание Python задачи"""
        def dynamic_function(**kwargs):
            # Динамический импорт и выполнение функции
            import importlib.util
            spec = importlib.util.spec_from_file_location("module.name", file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            func = getattr(module, function_name)
            return func(**kwargs)
        
        return PythonOperator(
            task_id=task_id,
            python_callable=dynamic_function,
            dag=dag
        )
    
    def _create_sql_task(self, dag, sql_command, task_id):
    # Просто логируем все SQL команды
        bash_cmd = f"echo '📝 SQL Command: {sql_command[:200]}...'"
    
        return BashOperator(
        task_id=task_id,
        bash_command=bash_cmd,
        dag=dag
    )
    
    
    def _get_schedule_info(self, file_path):
        """Извлечение информации о расписании из комментариев файла"""
        schedule_patterns = {
            'schedule_interval': r'#\s*schedule_interval:\s*(.+)',
            'start_date': r'#\s*start_date:\s*(.+)',
            'catchup': r'#\s*catchup:\s*(true|false)'
        }
        
        schedule_info = {
            'schedule_interval': '@daily',
            'start_date': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
            'catchup': False
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            for key, pattern in schedule_patterns.items():
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    if key == 'schedule_interval':
                        schedule_info[key] = match.group(1).strip()
                    elif key == 'start_date':
                        try:
                            schedule_info[key] = datetime.strptime(match.group(1).strip(), '%Y-%m-%d')
                        except:
                            pass
                    elif key == 'catchup':
                        schedule_info[key] = match.group(1).lower() == 'true'
        
        except Exception as e:
            logger.warning(f"Error reading schedule info from {file_path}: {e}")
        
        return schedule_info
    
    def generate_dag_from_file(self, file_path):
        """Генерация DAG из файла"""
        filename = os.path.basename(file_path)
        dag_id = os.path.splitext(filename)[0]
        
        # Получение информации о расписании
        schedule_info = self._get_schedule_info(file_path)
        
        # Создание DAG
        dag = DAG(
            dag_id=dag_id,
            default_args={
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
            },
            schedule_interval=schedule_info['schedule_interval'],
            start_date=schedule_info['start_date'],
            catchup=schedule_info['catchup'],
            tags=['auto_generated'],
        )
        
        extension = os.path.splitext(filename)[1].lower()
        
        if extension == '.py':
            # Обработка Python файла
            functions = self._parse_python_file(file_path)
            
            start_task = EmptyOperator(task_id="start", dag=dag)
            end_task = EmptyOperator(task_id="end", dag=dag)
            
            previous_task = start_task
            for i, func in enumerate(functions):
                task = self._create_python_task(
                    dag, file_path, func['name'], f"python_task_{i+1}_{func['name']}"
                )
                previous_task >> task
                previous_task = task
            
            previous_task >> end_task
            
        elif extension == '.sql':
            # Обработка SQL файла
            commands = self._parse_sql_file(file_path)
            
            start_task = EmptyOperator(task_id="start", dag=dag)
            end_task = EmptyOperator(task_id="end", dag=dag)
            
            previous_task = start_task
            for i, command in enumerate(commands):
                if command.strip():
                    task = self._create_sql_task(
                        dag, command, f"sql_task_{i+1}"
                    )
                    previous_task >> task
                    previous_task = task
            
            previous_task >> end_task
        
        elif extension in ['.jar']:
            # Обработка Spark JAR
            start_task = EmptyOperator(task_id="start", dag=dag)
            spark_task = self._create_spark_task(dag, file_path, "spark_job")
            end_task = EmptyOperator(task_id="end", dag=dag)
            
            start_task >> spark_task >> end_task
        
        return dag
    
    def generate_all_dags(self):
        """Генерация всех DAG'ов из папки sources"""
        dags = []
        
        # Поиск всех подходящих файлов
        patterns = ['*.py', '*.sql', '*.jar']
        
        for pattern in patterns:
            for file_path in glob.glob(os.path.join(self.sources_dir, pattern)):
                try:
                    dag = self.generate_dag_from_file(file_path)
                    dags.append(dag)
                    logger.info(f"Successfully generated DAG from {file_path}")
                except Exception as e:
                    logger.error(f"Error generating DAG from {file_path}: {e}")
        
        return dags

# Глобальная переменная для хранения сгенерированных DAG'ов
generated_dags = {}

# Инициализация генератора
dag_generator = DAGGenerator(sources_dir="/opt/airflow/sources")

# Генерация DAG'ов при загрузке модуля
for dag in dag_generator.generate_all_dags():
    generated_dags[dag.dag_id] = dag
    globals()[dag.dag_id] = dag