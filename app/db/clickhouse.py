"""
Модуль для работы с ClickHouse
"""
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
import pandas as pd

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from app.models.service_db import TableStructure
from app.config import CLICKHOUSE

logger = logging.getLogger(__name__)


def mysql_type_to_clickhouse(mysql_type: str) -> str:
    """Преобразует тип данных MySQL в тип данных ClickHouse
    
    Args:
        mysql_type: Тип данных MySQL
        
    Returns:
        Эквивалентный тип данных ClickHouse
    """
    mysql_type = mysql_type.lower()
    
    # Целые числа
    if 'tinyint' in mysql_type:
        return 'Int8'
    elif 'smallint' in mysql_type:
        return 'Int16'
    elif 'mediumint' in mysql_type:
        return 'Int32'
    elif 'int' in mysql_type and not ('bigint' in mysql_type):
        return 'Int32'
    elif 'bigint' in mysql_type:
        return 'Int64'
        
    # Числа с плавающей точкой
    elif 'float' in mysql_type:
        return 'Float32'
    elif 'double' in mysql_type or 'real' in mysql_type:
        return 'Float64'
    elif 'decimal' in mysql_type:
        # Извлекаем точность и масштаб из определения типа decimal
        try:
            precision, scale = mysql_type.split('(')[1].split(')')[0].split(',')
            return f'Decimal({precision.strip()}, {scale.strip()})'
        except:
            return 'Decimal(38, 10)'  # Значения по умолчанию
        
    # Строковые типы
    elif 'char' in mysql_type or 'text' in mysql_type or 'blob' in mysql_type:
        return 'String'
        
    # Датa и время
    elif 'date' in mysql_type and not ('datetime' in mysql_type):
        return 'Date'
    elif 'datetime' in mysql_type or 'timestamp' in mysql_type:
        return 'DateTime'
        
    # Булев тип
    elif 'bool' in mysql_type or 'boolean' in mysql_type:
        return 'UInt8'  # В ClickHouse нет отдельного типа Boolean
        
    # JSON
    elif 'json' in mysql_type:
        return 'String'  # Можно использовать JSON в некоторых версиях ClickHouse
        
    # По умолчанию для неизвестных типов
    return 'String'


class ClickHouseClient:
    """Клиент для работы с ClickHouse"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Инициализация клиента для ClickHouse
        
        Args:
            config: Настройки подключения к ClickHouse
        """
        self.config = config or CLICKHOUSE
        self.client = None
        
    def connect(self):
        """Устанавливает соединение с ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            logger.info(f"Подключение к ClickHouse установлено: {self.config['host']}:{self.config['port']}")
        except ClickHouseError as err:
            logger.error(f"Ошибка подключения к ClickHouse: {err}")
            raise
            
    def disconnect(self):
        """Закрывает соединение с ClickHouse"""
        if self.client:
            self.client = None
            logger.info("Соединение с ClickHouse закрыто")
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        
    def create_table(self, database_name: str, table_structure: TableStructure, engine: str = 'MergeTree()') -> bool:
        """Создает таблицу в ClickHouse на основе структуры MySQL
        
        Args:
            database_name: Имя базы данных-источника (используется как префикс для таблицы)
            table_structure: Структура таблицы
            engine: Движок таблицы ClickHouse
            
        Returns:
            True если таблица создана успешно
        """
        if not self.client:
            self.connect()
            
        # Формируем имя таблицы с префиксом базы данных для уникальности
        table_name = f"{database_name}__{table_structure.name}"
        
        # Формируем SQL для создания таблицы
        columns_sql = []
        for column in table_structure.columns:
            ch_type = mysql_type_to_clickhouse(column['type'])
            nullable = 'Nullable(' + ch_type + ')' if column['nullable'] else ch_type
            columns_sql.append(f"`{column['name']}` {nullable}")
        
        # Добавляем поле для обозначения источника данных
        columns_sql.append("`_source_database` String")
        
        # Определяем первичный ключ или используем ORDER BY
        order_by = ''
        if table_structure.primary_key:
            primary_key_cols = [f"`{col}`" for col in table_structure.primary_key]
            order_by = f"ORDER BY ({', '.join(primary_key_cols)})"
        else:
            # Если первичного ключа нет, используем первую колонку
            order_by = f"ORDER BY (`{table_structure.columns[0]['name']}`)"
            
        # Формируем полный SQL запрос
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.config['database']}`.`{table_name}` (
            {','.join(columns_sql)}
        ) ENGINE = {engine} {order_by}
        """
        
        try:
            self.client.command(create_sql)
            logger.info(f"Создана таблица '{table_name}' в ClickHouse")
            return True
        except ClickHouseError as err:
            logger.error(f"Ошибка создания таблицы '{table_name}' в ClickHouse: {err}")
            raise
    
    def table_exists(self, database_name: str, table_name: str) -> bool:
        """Проверяет существование таблицы в ClickHouse
        
        Args:
            database_name: Имя базы данных-источника (используется как префикс для таблицы)
            table_name: Имя таблицы
            
        Returns:
            True если таблица существует
        """
        if not self.client:
            self.connect()
            
        full_table_name = f"{database_name}__{table_name}"
        
        try:
            query = f"""
            SELECT name FROM system.tables 
            WHERE database = '{self.config['database']}' AND name = '{full_table_name}'
            """
            result = self.client.query(query)
            return len(result.result_rows) > 0
        except ClickHouseError as err:
            logger.error(f"Ошибка проверки существования таблицы '{full_table_name}': {err}")
            return False
    
    def insert_data(self, database_name: str, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Вставляет данные в таблицу ClickHouse
        
        Args:
            database_name: Имя базы данных-источника (используется как префикс для таблицы)
            table_name: Имя таблицы
            data: Список записей для вставки
            
        Returns:
            Количество вставленных записей
        """
        if not self.client:
            self.connect()
            
        if not data:
            logger.warning("Вставка пропущена: нет данных для вставки")
            return 0
            
        full_table_name = f"{database_name}__{table_name}"
        
        try:
            # Добавляем поле источника к каждой записи
            for row in data:
                row['_source_database'] = database_name
                
            # Используем pandas DataFrame для вставки данных
            df = pd.DataFrame(data)
            result = self.client.insert_df(f"{self.config['database']}.{full_table_name}", df)
            
            logger.info(f"Вставлено {len(data)} записей в таблицу '{full_table_name}'")
            return len(data)
        except ClickHouseError as err:
            logger.error(f"Ошибка вставки данных в таблицу '{full_table_name}': {err}")
            raise
            
    def delete_data(self, database_name: str, table_name: str, condition: Optional[str] = None) -> int:
        """Удаляет данные из таблицы ClickHouse
        
        Args:
            database_name: Имя базы данных-источника (используется как префикс для таблицы)
            table_name: Имя таблицы
            condition: Условие для удаления (WHERE)
            
        Returns:
            Количество затронутых записей (ClickHouse не возвращает это число, поэтому всегда 0)
        """
        if not self.client:
            self.connect()
            
        full_table_name = f"{database_name}__{table_name}"
        
        try:
            query = f"DELETE FROM {self.config['database']}.{full_table_name}"
            if condition:
                query += f" WHERE {condition}"
            else:
                query += f" WHERE _source_database = '{database_name}'"
                
            self.client.command(query)
            logger.info(f"Удалены данные из таблицы '{full_table_name}' с условием: {condition or '_source_database = ' + database_name}")
            return 0  # ClickHouse не возвращает количество удаленных записей
        except ClickHouseError as err:
            logger.error(f"Ошибка удаления данных из таблицы '{full_table_name}': {err}")
            raise
            
    def get_row_count(self, database_name: str, table_name: str, condition: Optional[str] = None) -> int:
        """Получает количество строк в таблице ClickHouse
        
        Args:
            database_name: Имя базы данных-источника (используется как префикс для таблицы)
            table_name: Имя таблицы
            condition: Дополнительное условие для подсчета (WHERE)
            
        Returns:
            Количество строк
        """
        if not self.client:
            self.connect()
            
        full_table_name = f"{database_name}__{table_name}"
        
        try:
            query = f"SELECT COUNT() FROM {self.config['database']}.{full_table_name}"
            if condition:
                query += f" WHERE {condition}"
            else:
                query += f" WHERE _source_database = '{database_name}'"
                
            result = self.client.query(query)
            return result.result_rows[0][0] if result.result_rows else 0
        except ClickHouseError as err:
            logger.error(f"Ошибка получения количества строк в таблице '{full_table_name}': {err}")
            return 0 