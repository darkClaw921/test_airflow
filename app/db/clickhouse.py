"""
Модуль для работы с ClickHouse
"""
import time
from typing import List, Dict, Any, Optional, Union, Tuple
import pandas as pd
import uuid

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from loguru import logger

from app.models.service_db import TableStructure
from app.config import CLICKHOUSE
from app.db.merge_config import get_key_fields


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
        
    def create_table(self, database_name: str, table_structure: TableStructure, engine: str = None) -> bool:
        """Создает таблицу в ClickHouse с движком ReplacingMergeTree
        
        Args:
            database_name: Имя базы данных-источника (используется как префикс для таблицы)
            table_structure: Структура таблицы
            engine: Движок таблицы ClickHouse (если не указан, используется ReplacingMergeTree)
            
        Returns:
            True если таблица создана успешно
        """
        if not self.client:
            self.connect()
            
        table_name = f"{table_structure.name}"
        
        # Формируем SQL для создания таблицы
        columns_sql = []
        for column in table_structure.columns:
            ch_type = mysql_type_to_clickhouse(column['type'])
            nullable = 'Nullable(' + ch_type + ')' if column['nullable'] else ch_type
            columns_sql.append(f"`{column['name']}` {nullable}")
        
        # Добавляем служебные поля
        columns_sql.append("`_source_database` String")
        columns_sql.append("`_version` UInt64")  # Версионный столбец для ReplacingMergeTree
        
        # Получаем ключевые поля для ORDER BY
        column_names = [col['name'] for col in table_structure.columns]
        key_fields = get_key_fields(table_structure.name, column_names)
        
        # Формируем ORDER BY с ключевыми полями
        order_by_fields = [f"`{field}`" for field in key_fields if field in column_names]
        if not order_by_fields:
            # Если ключевые поля не найдены, используем первую колонку
            order_by_fields = [f"`{table_structure.columns[0]['name']}`"]
        
        order_by = f"ORDER BY ({', '.join(order_by_fields)})"
        
        # Используем ReplacingMergeTree с версионным столбцом
        if not engine:
            engine = "ReplacingMergeTree(_version)"
            
        # Формируем полный SQL запрос
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.config['database']}`.`{table_name}` (
            {','.join(columns_sql)}
        ) ENGINE = {engine} {order_by}
        """
        
        try:
            self.client.command(create_sql)
            logger.info(f"Создана таблица '{table_name}' в ClickHouse с движком {engine}")
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
            
        full_table_name = f"{table_name}"
        
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
            
        full_table_name = f"{table_name}"
        
        try:
            # Добавляем служебные поля к каждой записи
            current_version = int(time.time() * 1000000)  # Микросекунды как версия
            for row in data:
                row['_source_database'] = database_name
                row['_version'] = current_version
                
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
            
        full_table_name = f"{table_name}"
        
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
            
        full_table_name = f"{table_name}"
        
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
            
    def merge_data(self, database_name: str, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Вставляет или обновляет данные в таблице ClickHouse используя ReplacingMergeTree
        
        Для ReplacingMergeTree просто делаем INSERT с новой версией.
        Движок автоматически заменит старые записи новыми при слиянии.
        
        Args:
            database_name: Имя базы данных-источника (используется как префикс для таблицы)
            table_name: Имя таблицы
            data: Список записей для вставки/обновления
            
        Returns:
            Количество обработанных записей
        """
        if not self.client:
            self.connect()
            
        if not data:
            logger.warning("Синхронизация пропущена: нет данных для обработки")
            return 0
            
        full_table_name = f"{table_name}"
        target_table = f"{self.config['database']}.{full_table_name}"
        
        try:
            # Добавляем служебные поля к каждой записи
            current_version = int(time.time() * 1000000)  # Микросекунды как версия
            for row in data:
                row['_source_database'] = database_name
                row['_version'] = current_version
                
            # Простая вставка - ReplacingMergeTree сам разберется с дублями
            df = pd.DataFrame(data)
            self.client.insert_df(target_table, df)
            
            logger.info(f"Обновлено/добавлено {len(data)} записей в таблицу '{full_table_name}' с версией {current_version}")
            return len(data)
            
        except ClickHouseError as err:
            logger.error(f"Ошибка при обновлении данных в таблице '{full_table_name}': {err}")
            raise

    def batch_merge_data(self, database_name: str, table_name: str, data: List[Dict[str, Any]], 
                    batch_size: int = 10000) -> int:
        """Вставляет или обновляет данные в таблице ClickHouse с пакетной обработкой
        
        Разбивает большой набор данных на пакеты для более эффективной обработки
        
        Args:
            database_name: Имя базы данных-источника (используется как префикс для таблицы)
            table_name: Имя таблицы
            data: Список записей для вставки/обновления
            batch_size: Размер пакета для обработки за один раз
            
        Returns:
            Общее количество обработанных записей
        """
        if not data:
            logger.warning("Пакетная синхронизация пропущена: нет данных для обработки")
            return 0
            
        total_records = len(data)
        processed_records = 0
        
        # Создаем единую версию для всего batch для консистентности
        batch_version = int(time.time() * 1000000)
        
        # Разбиваем данные на пакеты
        for i in range(0, total_records, batch_size):
            batch = data[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_records + batch_size - 1) // batch_size
            
            logger.info(f"Обработка пакета {batch_num}/{total_batches} ({len(batch)} записей) для {database_name}/{table_name}")
            
            try:
                # Устанавливаем версию для текущего пакета (инкрементируем для каждого пакета)
                current_batch_version = batch_version + batch_num
                
                # Добавляем служебные поля
                for row in batch:
                    row['_source_database'] = database_name
                    row['_version'] = current_batch_version
                
                # Вставляем пакет
                full_table_name = f"{table_name}"
                target_table = f"{self.config['database']}.{full_table_name}"
                df = pd.DataFrame(batch)
                self.client.insert_df(target_table, df)
                
                processed_records += len(batch)
                logger.info(f"Пакет {batch_num}/{total_batches} успешно обработан ({len(batch)} записей)")
                
            except Exception as err:
                logger.error(f"Ошибка обработки пакета {batch_num}/{total_batches}: {err}")
                raise
        
        logger.info(f"Всего обработано {processed_records} записей для {database_name}/{table_name}")
        return processed_records
        
    def optimize_table(self, table_name: str) -> bool:
        """Принудительно запускает слияние для таблицы ReplacingMergeTree
        
        Полезно для немедленного применения замен после вставки данных
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            True если операция прошла успешно
        """
        if not self.client:
            self.connect()
            
        try:
            query = f"OPTIMIZE TABLE {self.config['database']}.{table_name} FINAL"
            self.client.command(query)
            logger.info(f"Запущено слияние для таблицы '{table_name}'")
            return True
        except ClickHouseError as err:
            logger.error(f"Ошибка слияния таблицы '{table_name}': {err}")
            return False
            
    def get_latest_data(self, table_name: str, condition: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Получает актуальные данные из таблицы ReplacingMergeTree
        
        Использует FINAL для получения только последних версий записей
        
        Args:
            table_name: Имя таблицы
            condition: Дополнительное условие WHERE
            limit: Ограничение количества записей
            
        Returns:
            Список актуальных записей
        """
        if not self.client:
            self.connect()
            
        try:
            query = f"SELECT * FROM {self.config['database']}.{table_name} FINAL"
            if condition:
                query += f" WHERE {condition}"
            if limit:
                query += f" LIMIT {limit}"
                
            result = self.client.query(query)
            
            # Преобразуем результат в список словарей
            if result.result_rows:
                columns = [col[0] for col in result.column_names]
                return [dict(zip(columns, row)) for row in result.result_rows]
            return []
            
        except ClickHouseError as err:
            logger.error(f"Ошибка получения данных из таблицы '{table_name}': {err}")
            return [] 