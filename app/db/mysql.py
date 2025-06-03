"""
Модуль для работы с MySQL базами данных
"""
import logging
from typing import List, Dict, Any, Optional, Tuple

import mysql.connector
from mysql.connector import Error as MySQLError

from app.models.service_db import MySQLConnection, TableStructure
from app.config import SERVICE_DB, SERVICE_CONNECTIONS_TABLE, BATCH_SIZE

logger = logging.getLogger(__name__)


class ServiceDBClient:
    """Клиент для работы с сервисной базой данных"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Инициализация клиента для сервисной БД
        
        Args:
            config: Настройки подключения к сервисной БД
        """
        self.config = config or SERVICE_DB
        self.connection = None
        
    def connect(self):
        """Устанавливает соединение с сервисной БД"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            logger.info(f"Подключение к сервисной БД установлено: {self.config['host']}:{self.config['port']}")
        except MySQLError as err:
            logger.error(f"Ошибка подключения к сервисной БД: {err}")
            raise
            
    def disconnect(self):
        """Закрывает соединение с сервисной БД"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Соединение с сервисной БД закрыто")
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
    
    def get_mysql_connections(self, enabled_only: bool = True) -> List[MySQLConnection]:
        """Получает список настроек подключений к БД MySQL из сервисной таблицы
        
        Args:
            enabled_only: Только активные подключения
            
        Returns:
            Список объектов MySQLConnection
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()
            
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = f"SELECT * FROM {SERVICE_CONNECTIONS_TABLE}"
            if enabled_only:
                query += " WHERE enabled = 1"
            
            cursor.execute(query)
            connections = [MySQLConnection.from_dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Получено {len(connections)} настроек подключений к MySQL")
            return connections
            
        except MySQLError as err:
            logger.error(f"Ошибка получения списка подключений: {err}")
            raise


class MySQLClient:
    """Клиент для работы с конкретной MySQL базой данных"""
    
    def __init__(self, connection: MySQLConnection):
        """Инициализация клиента для работы с MySQL
        
        Args:
            connection: Настройки подключения к MySQL
        """
        self.connection_config = connection
        self.db_connection = None
        
    def connect(self):
        """Устанавливает соединение с MySQL"""
        try:
            conn_params = {
                'host': self.connection_config.host,
                'port': self.connection_config.port,
                'user': self.connection_config.user,
                'password': self.connection_config.password,
                'database': self.connection_config.database
            }
            self.db_connection = mysql.connector.connect(**conn_params)
            logger.info(f"Подключение к MySQL БД '{self.connection_config.name}' установлено")
        except MySQLError as err:
            logger.error(f"Ошибка подключения к MySQL БД '{self.connection_config.name}': {err}")
            raise
            
    def disconnect(self):
        """Закрывает соединение с MySQL"""
        if self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
            logger.info(f"Соединение с MySQL БД '{self.connection_config.name}' закрыто")
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        
    def get_table_structure(self, table_name: str) -> TableStructure:
        """Получает структуру указанной таблицы
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Объект с описанием структуры таблицы
        """
        if not self.db_connection or not self.db_connection.is_connected():
            self.connect()
            
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            cursor.execute(f"DESCRIBE {table_name}")
            columns_desc = cursor.fetchall()
            cursor.close()
            
            table_structure = TableStructure.from_desc(table_name, columns_desc)
            logger.info(f"Получена структура таблицы '{table_name}': {len(table_structure.columns)} колонок")
            return table_structure
            
        except MySQLError as err:
            logger.error(f"Ошибка получения структуры таблицы '{table_name}': {err}")
            raise
            
    def get_tables(self) -> List[str]:
        """Получает список всех таблиц в базе данных
        
        Returns:
            Список имен таблиц
        """
        if not self.db_connection or not self.db_connection.is_connected():
            self.connect()
            
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Получен список таблиц ({len(tables)}): {', '.join(tables)}")
            return tables
            
        except MySQLError as err:
            logger.error(f"Ошибка получения списка таблиц: {err}")
            raise
            
    def read_data_batch(self, table_name: str, offset: int = 0, limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]], int]:
        """Читает пакет данных из таблицы
        
        Args:
            table_name: Имя таблицы
            offset: Смещение от начала таблицы
            limit: Максимальное количество записей (по умолчанию BATCH_SIZE)
            
        Returns:
            Кортеж (данные, количество записей)
        """
        if not self.db_connection or not self.db_connection.is_connected():
            self.connect()
            
        try:
            cursor = self.db_connection.cursor(dictionary=True)
            limit_val = limit or BATCH_SIZE
            query = f"SELECT * FROM {table_name} LIMIT {offset}, {limit_val}"
            cursor.execute(query)
            batch_data = cursor.fetchall()
            record_count = len(batch_data)
            cursor.close()
            
            logger.info(f"Прочитано {record_count} записей из таблицы '{table_name}' (смещение: {offset})")
            return batch_data, record_count
            
        except MySQLError as err:
            logger.error(f"Ошибка чтения данных из таблицы '{table_name}': {err}")
            raise
            
    def get_row_count(self, table_name: str) -> int:
        """Получает количество строк в таблице
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Количество строк
        """
        if not self.db_connection or not self.db_connection.is_connected():
            self.connect()
            
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            cursor.close()
            return row_count
            
        except MySQLError as err:
            logger.error(f"Ошибка получения количества строк в таблице '{table_name}': {err}")
            raise 