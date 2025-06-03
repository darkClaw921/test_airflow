"""
Сервис для обработки данных
"""
import logging
from typing import List, Dict, Any, Optional, Tuple

from app.models.service_db import TableStructure
from app.db.mysql import MySQLClient
from app.db.clickhouse import ClickHouseClient
from app.config import BATCH_SIZE

logger = logging.getLogger(__name__)


class DataService:
    """Сервис для обработки данных из MySQL в ClickHouse"""
    
    def __init__(self, mysql_client: MySQLClient, clickhouse_client: ClickHouseClient):
        """Инициализация сервиса обработки данных
        
        Args:
            mysql_client: Клиент для работы с MySQL
            clickhouse_client: Клиент для работы с ClickHouse
        """
        self.mysql_client = mysql_client
        self.clickhouse_client = clickhouse_client
        
    def ensure_table_exists(self, table_name: str) -> TableStructure:
        """Проверяет существование таблицы в ClickHouse и создает ее при необходимости
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Структура таблицы
        """
        # database_name = self.mysql_client.connection_config.database
        database_name = self.mysql_client.connection_config.name
        
        # Получаем структуру таблицы из MySQL
        table_structure = self.mysql_client.get_table_structure(table_name)
        
        # Проверяем существование таблицы в ClickHouse
        if not self.clickhouse_client.table_exists(database_name, table_name):
            logger.info(f"Таблица '{table_name}' не существует в ClickHouse, создаем...")
            self.clickhouse_client.create_table(database_name, table_structure)
        else:
            logger.info(f"Таблица '{table_name}' уже существует в ClickHouse")
            
        return table_structure
        
    def process_table(self, table_name: str) -> Tuple[int, int]:
        """Обрабатывает таблицу MySQL и передает данные в ClickHouse
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Кортеж (прочитано записей, записано в ClickHouse)
        """
        # database_name = self.mysql_client.connection_config.database
        database_name = self.mysql_client.connection_config.name
        
        # Убедимся, что таблица существует в ClickHouse
        self.ensure_table_exists(table_name)
        
        # Получаем общее количество строк для обработки
        total_rows = self.mysql_client.get_row_count(table_name)
        logger.info(f"Всего строк в таблице '{table_name}': {total_rows}")
        
        # Обрабатываем данные пакетами
        offset = 0
        total_processed = 0
        total_written = 0
        
        # Собираем все пакеты данных для эффективной обработки
        all_data = []
        
        while True:
            # Читаем пакет данных из MySQL
            batch_data, records_read = self.mysql_client.read_data_batch(table_name, offset, BATCH_SIZE)
            
            if records_read == 0:
                break
                
            total_processed += records_read
            all_data.extend(batch_data)
            
            logger.info(f"Прогресс чтения: {total_processed}/{total_rows} строк ({int(total_processed/total_rows*100) if total_rows else 100}%)")
            
            # Переходим к следующему пакету
            offset += BATCH_SIZE
            
            if records_read < BATCH_SIZE:
                break
        
        # Используем batch_merge_data для обновления или вставки данных
        if all_data:
            total_written = self.clickhouse_client.batch_merge_data(
                database_name=database_name,
                table_name=table_name,
                data=all_data,
                batch_size=50000  # Оптимальный размер пакета
            )
                
        logger.info(f"Обработка таблицы '{table_name}' завершена: прочитано {total_processed}, обработано {total_written}")
        return total_processed, total_written 