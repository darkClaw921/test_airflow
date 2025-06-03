# import sys
# import os
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from clickhouse_connect import get_client
# import pandas as pd
# from dotenv import load_dotenv

# load_dotenv()

# # Добавляем путь к основному приложению
# sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# from app.db.mysql import ServiceDBClient, MySQLClient
# from app.db.clickhouse import ClickHouseClient
# from app.config import CLICKHOUSE
# from loguru import logger

# # Настройка логирования
# logger.add("logs/mysql_to_clickhouse_etl.log", rotation="10 MB", level="INFO")

# # Настройки DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Создание DAG
# dag = DAG(
#     'mysql_to_clickhouse_etl',
#     default_args=default_args,
#     description='ETL процесс из MySQL в ClickHouse',
#     schedule=timedelta(days=1),
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
#     tags=['etl', 'mysql', 'clickhouse'],
# )

# # Функция для извлечения данных из MySQL
# def extract_mysql_data(table_name, **kwargs):
#     logger.info(f"Извлечение данных из таблицы MySQL: {table_name}")
    
#     try:
#         # Получаем первое активное соединение MySQL из сервисной БД
#         with ServiceDBClient() as service_client:
#             mysql_connections = service_client.get_mysql_connections(enabled_only=True)
            
#         if not mysql_connections:
#             logger.error("Не найдено активных соединений MySQL")
#             raise ValueError("Не найдено активных соединений MySQL")
        
#         # Используем первое доступное соединение
#         mysql_connection = mysql_connections[0]
#         logger.info(f"Используется соединение: {mysql_connection.name}")
        
#         # Получаем данные из MySQL, используя собственную реализацию клиента
#         with MySQLClient(mysql_connection) as mysql_client:
#             row_count = mysql_client.get_row_count(table_name)
#             logger.info(f"Всего строк в таблице {table_name}: {row_count}")
            
#             data, read_count = mysql_client.read_data_batch(table_name)
        
#         # Преобразуем данные в DataFrame
#         df = pd.DataFrame(data)
        
#         logger.info(f"Извлечено {len(df)} записей из таблицы {table_name}")
        
#         # Проверяем, что есть данные для сохранения
#         if len(df) > 0:
#             # Сохраняем DataFrame в XCom для использования в следующих задачах
#             data_json = df.to_json(orient='records')
#             kwargs['ti'].xcom_push(key=f'mysql_data_{table_name}', value=data_json)
#             logger.info(f"Данные для таблицы {table_name} сохранены в XCom")
#         else:
#             logger.warning(f"Таблица {table_name} не содержит данных")
#             # Сохраняем пустой список, чтобы не было None
#             kwargs['ti'].xcom_push(key=f'mysql_data_{table_name}', value='[]')
            
#         return f"Извлечение данных из {table_name} завершено успешно"
#     except Exception as e:
#         logger.error(f"Ошибка при извлечении данных из {table_name}: {str(e)}")
#         raise

# # Функция для загрузки данных в ClickHouse
# def load_to_clickhouse(table_name, **kwargs):
#     logger.info(f"Загрузка данных в таблицу ClickHouse: {table_name}")
    
#     try:
#         # Получаем данные из XCom
#         ti = kwargs['ti']
#         json_data = ti.xcom_pull(key=f'mysql_data_{table_name}', task_ids=f'extract_{table_name}')
        
#         # Проверяем, что данные получены
#         if json_data is None:
#             logger.error(f"Данные для таблицы {table_name} не найдены в XCom")
#             raise ValueError(f"Данные для таблицы {table_name} не найдены в XCom")
        
#         # Преобразуем JSON в список словарей
#         data = pd.read_json(json_data, orient='records').to_dict('records')
        
#         # Проверяем наличие данных для загрузки
#         if len(data) == 0:
#             logger.warning(f"Нет данных для загрузки в таблицу {table_name}")
#             return f"Загрузка в {table_name} пропущена: нет данных"
        
#         # Получаем первое активное соединение MySQL из сервисной БД для имени базы данных
#         with ServiceDBClient() as service_client:
#             mysql_connections = service_client.get_mysql_connections(enabled_only=True)
        
#         if not mysql_connections:
#             logger.error("Не найдено активных соединений MySQL")
#             raise ValueError("Не найдено активных соединений MySQL")
        
#         # Используем имя первого доступного соединения как имя базы данных-источника
#         database_name = mysql_connections[0].name
        
#         # Создаем клиент ClickHouse и используем новый метод merge_data
#         with ClickHouseClient() as clickhouse_client:
#             # Используем пакетную обработку для больших объемов данных
#             processed = clickhouse_client.batch_merge_data(
#                 database_name=database_name, 
#                 table_name=table_name,
#                 data=data,
#                 batch_size=50000  # Оптимальный размер пакета
#             )
            
#         logger.info(f"Обработано {processed} записей в таблице {table_name} в ClickHouse")
        
#         return f"Загрузка данных в {table_name} завершена успешно (обработано {processed} записей)"
#     except Exception as e:
#         logger.error(f"Ошибка при загрузке данных в {table_name}: {str(e)}")
#         raise

# # Создаем задачи для каждой таблицы
# tables = ['customers', 'products', 'orders', 'order_items']

# for table in tables:
#     extract_task = PythonOperator(
#         task_id=f'extract_{table}',
#         python_callable=extract_mysql_data,
#         op_kwargs={'table_name': table},
#         dag=dag,
#     )
    
#     load_task = PythonOperator(
#         task_id=f'load_{table}',
#         python_callable=load_to_clickhouse,
#         op_kwargs={'table_name': table},
#         dag=dag,
#     )
    
#     # Определяем зависимости между задачами
#     extract_task >> load_task

# # Функция для обновления материализованных представлений в ClickHouse
# def refresh_materialized_views(**kwargs):
#     logger.info("Обновление материализованных представлений в ClickHouse")
    
#     try:
#         # Создаем клиент ClickHouse
#         with ClickHouseClient() as clickhouse_client:
#             # Список материализованных представлений для обновления
#             views = [
#                 'mv_category_sales',
#                 'mv_daily_sales',
#                 'mv_customer_activity',
#                 'mv_product_popularity'
#             ]
            
#             for view in views:
#                 try:
#                     # Выполняем запрос для обновления представления
#                     clickhouse_client.client.command(f"OPTIMIZE TABLE {clickhouse_client.config['database']}.{view} FINAL")
#                     logger.info(f"Представление {view} успешно обновлено")
#                 except Exception as e:
#                     logger.error(f"Ошибка при обновлении представления {view}: {e}")
            
#         return "Обновление материализованных представлений завершено"
#     except Exception as e:
#         logger.error(f"Ошибка при обновлении материализованных представлений: {str(e)}")
#         raise

# # Создаем задачу для обновления материализованных представлений
# refresh_views_task = PythonOperator(
#     task_id='refresh_materialized_views',
#     python_callable=refresh_materialized_views,
#     dag=dag,
# )

# # Задаем зависимости для обновления представлений
# for table in tables:
#     dag.get_task(f'load_{table}') >> refresh_views_task 