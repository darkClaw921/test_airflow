"""
Сервис для синхронизации данных между MySQL и ClickHouse
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.db.mysql import ServiceDBClient, MySQLClient
from app.db.clickhouse import ClickHouseClient
from app.models.service_db import MySQLConnection
from app.services.data_service import DataService

logger = logging.getLogger(__name__)


class SyncResult:
    """Результат синхронизации данных"""
    
    def __init__(self, connection_name: str, database: str):
        self.connection_name = connection_name
        self.database = database
        self.start_time = datetime.now()
        self.end_time = None
        self.tables_processed = 0
        self.rows_read = 0
        self.rows_written = 0
        self.status = 'running'
        self.error = None
        
    def complete(self):
        """Отмечает завершение синхронизации"""
        self.end_time = datetime.now()
        self.status = 'completed'
        
    def fail(self, error_message: str):
        """Отмечает ошибку синхронизации"""
        self.end_time = datetime.now()
        self.status = 'failed'
        self.error = error_message
        
    def duration(self) -> float:
        """Возвращает продолжительность синхронизации в секундах"""
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()
        
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует результат в словарь"""
        return {
            'connection_name': self.connection_name,
            'database': self.database,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': self.duration(),
            'tables_processed': self.tables_processed,
            'rows_read': self.rows_read,
            'rows_written': self.rows_written,
            'status': self.status,
            'error': self.error
        }


class SyncService:
    """Сервис синхронизации данных из MySQL в ClickHouse"""
    
    def __init__(self):
        """Инициализация сервиса синхронизации"""
        self.service_db_client = ServiceDBClient()
        self.clickhouse_client = ClickHouseClient()
        
    def sync_connection(self, connection: MySQLConnection) -> SyncResult:
        """Синхронизирует данные из одного подключения MySQL в ClickHouse
        
        Args:
            connection: Настройки подключения к MySQL
            
        Returns:
            Результат синхронизации
        """
        result = SyncResult(connection.name, connection.database)
        logger.info(f"Начинаем синхронизацию данных из '{connection.name}' ({connection.database})")
        
        try:
            with MySQLClient(connection) as mysql_client, self.clickhouse_client:
                data_service = DataService(mysql_client, self.clickhouse_client)
                
                # Определяем список таблиц для синхронизации
                tables_to_sync = connection.tables or mysql_client.get_tables()
                
                for table_name in tables_to_sync:
                    logger.info(f"Обрабатываем таблицу '{table_name}'")
                    rows_read, rows_written = data_service.process_table(table_name)
                    
                    result.tables_processed += 1
                    result.rows_read += rows_read
                    result.rows_written += rows_written
                
                result.complete()
                logger.info(f"Синхронизация '{connection.name}' завершена успешно: "
                           f"{result.tables_processed} таблиц, {result.rows_read} строк прочитано, "
                           f"{result.rows_written} строк записано за {result.duration():.2f} секунд")
                
        except Exception as e:
            error_message = str(e)
            result.fail(error_message)
            logger.error(f"Ошибка синхронизации '{connection.name}': {error_message}")
            
        return result
        
    def sync_all(self) -> List[SyncResult]:
        """Синхронизирует данные из всех активных подключений MySQL в ClickHouse
        
        Returns:
            Список результатов синхронизации
        """
        results = []
        
        try:
            with self.service_db_client as client:
                connections = client.get_mysql_connections(enabled_only=True)
                
                if not connections:
                    logger.warning("Нет активных подключений для синхронизации")
                    return results
                    
                logger.info(f"Найдено {len(connections)} активных подключений для синхронизации")
                
                for connection in connections:
                    result = self.sync_connection(connection)
                    results.append(result)
                    
        except Exception as e:
            logger.error(f"Ошибка при получении списка подключений: {e}")
            
        return results 