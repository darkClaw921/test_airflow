# """
# DAG для синхронизации данных из MySQL в ClickHouse
# """
# import os
# import sys
# from datetime import datetime, timedelta
# import json
# from typing import Dict, Any

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.models import Variable
# # from airflow.utils.dates import days_ago

# # Добавляем путь к основному приложению
# sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# from app.services.sync_service import SyncService
# from app.db.mysql import ServiceDBClient
# from loguru import logger

# # Настройка логирования
# logger.add("logs/data_sync_dag.log", rotation="10 MB", level="INFO")

# # Аргументы DAG по умолчанию
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': True,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }


# def get_connection_count():
#     """Получает количество активных подключений MySQL из сервисной БД
    
#     Returns:
#         Словарь с информацией о подключениях
#     """
#     logger.info("Получение активных подключений MySQL")
#     with ServiceDBClient() as client:
#         connections = client.get_mysql_connections(enabled_only=True)
#         result = {
#             'connection_count': len(connections),
#             'connection_names': [conn.name for conn in connections]
#         }
#     logger.info(f"Найдено подключений: {result['connection_count']}")
#     return result


# def sync_data(**kwargs):
#     """Синхронизирует данные из MySQL в ClickHouse
    
#     Returns:
#         Строка с результатами синхронизации в формате JSON
#     """
#     logger.info("Начало синхронизации данных")
#     sync_service = SyncService()
#     results = sync_service.sync_all()
#     results_json = json.dumps([r.to_dict() for r in results])
    
#     # Сохраняем результат для передачи в XCom
#     kwargs['ti'].xcom_push(key='sync_results', value=results_json)
    
#     # Формируем краткую сводку
#     success_count = sum(1 for r in results if r.status == 'completed')
#     failed_count = len(results) - success_count
#     total_tables = sum(r.tables_processed for r in results)
#     total_rows = sum(r.rows_read for r in results)
    
#     summary = {
#         'success_count': success_count,
#         'failed_count': failed_count,
#         'total_connections': len(results),
#         'total_tables': total_tables,
#         'total_rows': total_rows
#     }
    
#     kwargs['ti'].xcom_push(key='sync_summary', value=json.dumps(summary))
#     logger.info(f"Синхронизация завершена: {success_count} успешно, {failed_count} с ошибками")
#     return results_json


# def analyze_results(**kwargs):
#     """Анализирует результаты синхронизации
    
#     Returns:
#         Строка с анализом результатов
#     """
#     logger.info("Анализ результатов синхронизации")
#     ti = kwargs['ti']
#     results_json = ti.xcom_pull(key='sync_results', task_ids='sync_data')
#     summary_json = ti.xcom_pull(key='sync_summary', task_ids='sync_data')
    
#     results = json.loads(results_json)
#     summary = json.loads(summary_json)
    
#     # Анализируем результаты
#     message_parts = []
#     message_parts.append(f"Всего обработано подключений: {summary['total_connections']}")
#     message_parts.append(f"Успешно: {summary['success_count']}")
#     message_parts.append(f"С ошибками: {summary['failed_count']}")
#     message_parts.append(f"Всего таблиц: {summary['total_tables']}")
#     message_parts.append(f"Всего строк: {summary['total_rows']}")
    
#     # Добавляем информацию о неудачных синхронизациях
#     failed = [r for r in results if r['status'] != 'completed']
#     if failed:
#         message_parts.append("\nОшибки синхронизации:")
#         for f in failed:
#             message_parts.append(f"- {f['connection_name']}: {f['error']}")
            
#     message = "\n".join(message_parts)
#     logger.info(message)
#     return message


# # Определение DAG
# with DAG(
#     'mysql_to_clickhouse_sync',
#     default_args=default_args,
#     description='Синхронизация данных из MySQL в ClickHouse',
#     schedule=timedelta(days=1),
#     start_date=datetime(2025, 6, 3),
#     catchup=False,
#     tags=['datastream', 'mysql', 'clickhouse', 'sync'],
# ) as dag:
    
#     # Проверка наличия подключений
#     check_connections = PythonOperator(
#         task_id='check_connections',
#         python_callable=get_connection_count,
#     )
    
#     # Синхронизация данных
#     sync_task = PythonOperator(
#         task_id='sync_data',
#         python_callable=sync_data,
#     )
    
#     # Анализ результатов
#     analyze_task = PythonOperator(
#         task_id='analyze_results',
#         python_callable=analyze_results,
#     )
    
#     # Определение порядка выполнения задач
#     check_connections >> sync_task >> analyze_task 