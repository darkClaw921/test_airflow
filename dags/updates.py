"""
DAG для обновления данных с отдельными задачами для каждого подключения и каждой таблицы
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from loguru import logger

# Добавляем путь к основному приложению
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from app.db.mysql import ServiceDBClient, MySQLClient
from app.db.clickhouse import ClickHouseClient
from app.services.data_service import DataService
from app.models.service_db import MySQLConnection

# Настройка логирования
logger.add("logs/updates_dag.log", rotation="10 MB", level="INFO")

# Аргументы DAG по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
    'updates_by_table',
    default_args=default_args,
    description='Обновление данных из MySQL в ClickHouse с отслеживанием по таблицам',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['datastream', 'mysql', 'clickhouse', 'updates'],
    render_template_as_native_obj=True,
) as dag:
    # Используем декораторы task для более простого определения задач
    @task
    def get_table_tasks():
        """
        Получает список всех таблиц для всех активных соединений MySQL
        
        Returns:
            Список словарей с параметрами таблиц для динамических задач
        """
        logger.info("Получение списка активных соединений MySQL и их таблиц")
        result = []
        
        with ServiceDBClient() as client:
            connections = client.get_mysql_connections(enabled_only=True)
            
        if not connections:
            logger.warning("Нет активных соединений для обновления данных")
            return result
            
        # Формируем плоский список всех таблиц из всех соединений
        for conn in connections:
            connection_id = conn.id
            connection_name = conn.name
            database = conn.database
            host = conn.host
            port = conn.port
            user = conn.user
            password = conn.password
            
            logger.info(f"Получение списка таблиц для соединения '{connection_name}'")
            
            # Получаем список таблиц
            try:
                with MySQLClient(conn) as mysql_client:
                    if conn.tables:
                        tables = conn.tables
                    else:
                        tables = mysql_client.get_tables()
                        
                logger.info(f"Для соединения '{connection_name}' найдено {len(tables)} таблиц")
                
                # Формируем параметры для каждой таблицы
                for table in tables:
                    result.append({
                        'connection_id': connection_id,
                        'connection_name': connection_name,
                        'database': database,
                        'host': host,
                        'port': port,
                        'user': user,
                        'password': password,
                        'table_name': table
                    })
            except Exception as e:
                logger.error(f"Ошибка при получении таблиц для соединения {connection_name}: {e}")
            
        logger.info(f"Всего найдено {len(result)} таблиц из {len(connections)} соединений")
        return result

    @task
    def sync_table(table_params: Dict):
        """
        Синхронизирует одну таблицу из MySQL в ClickHouse
        
        Args:
            table_params: Словарь с параметрами для синхронизации
            
        Returns:
            Словарь с результатами синхронизации
        """
        connection_id = table_params['connection_id']
        connection_name = table_params['connection_name']
        database = table_params['database']
        host = table_params['host']
        port = table_params['port']
        user = table_params['user']
        password = table_params['password']
        table_name = table_params['table_name']
        
        try:
            start_time = datetime.now()
            
            # Создаем объект подключения напрямую, без поиска по ID
            connection = MySQLConnection(
                id=connection_id,
                name=connection_name,
                database=database,
                host=host,
                port=port,
                user=user,
                password=password,
                enabled=True
            )
                
            logger.info(f"Начало синхронизации таблицы '{table_name}' из соединения '{connection_name}'")
            
            # Создаем клиенты для работы с базами данных
            with MySQLClient(connection) as mysql_client, ClickHouseClient() as clickhouse_client:
                # Создаем сервис для обработки данных
                data_service = DataService(mysql_client, clickhouse_client)
                
                # Синхронизируем таблицу
                rows_read, rows_written = data_service.process_table(table_name)
                
            # Вычисляем затраченное время
            end_time = datetime.now()
            duration_seconds = (end_time - start_time).total_seconds()
            
            # Формируем результат
            result = {
                "status": "completed",
                "connection": connection_name,
                "database": database,
                "table": table_name,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration_seconds
            }
            
            logger.info(f"Синхронизация таблицы '{table_name}' из соединения '{connection_name}' завершена: "
                      f"прочитано {rows_read} строк, записано {rows_written} строк за {duration_seconds:.2f} секунд")
                      
            return result
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"Ошибка при синхронизации таблицы '{table_name}': {error_message}")
            
            return {
                "status": "failed",
                "connection": connection_name,
                "database": database,
                "table": table_name,
                "error": error_message,
                "start_time": start_time.isoformat() if 'start_time' in locals() else None,
                "end_time": datetime.now().isoformat(),
                "duration_seconds": (datetime.now() - start_time).total_seconds() if 'start_time' in locals() else 0
            }
    
    @task
    def analyze_table_results(results):
        """
        Анализирует результаты синхронизации отдельных таблиц
        
        Args:
            results: Список результатов синхронизации таблиц
            
        Returns:
            Словарь с общей статистикой
        """
        if not results:
            return {
                "status": "completed",
                "tables_processed": 0,
                "rows_read": 0,
                "rows_written": 0,
                "failed_tables": 0
            }
        
        # Обработка списка списков результатов от распределенных задач
        all_results = []
        for batch in results:
            if isinstance(batch, list):
                all_results.extend(batch)
            else:
                all_results.append(batch)
        
        total_tables = len(all_results)
        total_rows_read = 0
        total_rows_written = 0
        failed_tables = 0
        
        for result in all_results:
            if result and result.get("status") == "completed":
                total_rows_read += result.get("rows_read", 0)
                total_rows_written += result.get("rows_written", 0)
            else:
                failed_tables += 1
                
        # Формируем общий результат
        summary = {
            "status": "completed",
            "tables_processed": total_tables,
            "rows_read": total_rows_read,
            "rows_written": total_rows_written,
            "failed_tables": failed_tables
        }
        
        logger.info(f"Анализ результатов синхронизации таблиц:")
        logger.info(f"  Всего таблиц: {total_tables}")
        logger.info(f"  Неуспешных таблиц: {failed_tables}")
        logger.info(f"  Прочитано строк: {total_rows_read}")
        logger.info(f"  Записано строк: {total_rows_written}")
        
        return summary
    
    # Упрощаем DAG, используя один источник данных для всех таблиц
    # 1. Получаем список всех таблиц для всех соединений
    table_tasks = get_table_tasks()
    
    # 2. Для каждой таблицы запускаем задачу синхронизации
    sync_results = sync_table.expand(table_params=table_tasks)
    
    # 3. Анализируем результаты
    analysis = analyze_table_results(sync_results)
    
    # 4. Финальная задача
    end = EmptyOperator(task_id='end')
    
    # Определяем зависимости
    analysis >> end
