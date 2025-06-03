import pendulum
import sys
import os
from time import sleep

# Добавляем корневую директорию проекта в пути импорта
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from airflow.sdk import DAG
from airflow.sdk import task
from loguru import logger

# Настройка логирования
logger.add("logs/test1_dag.log", rotation="10 MB", level="INFO")

def expensive_api_call():
    logger.info("Начало выполнения expensive_api_call")
    sleep(5)  # Уменьшил время для быстрого тестирования
    logger.info("Завершение выполнения expensive_api_call")
    return "Hello from Airflow!"


with DAG(
    dag_id="test1_dag",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    @task()
    def print_expensive_api_call():
        logger.info("Начало выполнения задачи")
        my_expensive_response = expensive_api_call()
        logger.info(f"Получен ответ: {my_expensive_response}")
        print(my_expensive_response)
        logger.info("Задача выполнена успешно")