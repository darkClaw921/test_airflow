"""
Модуль для работы с ClickHouse
"""
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
import pandas as pd
import uuid

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from app.models.service_db import TableStructure
from app.config import CLICKHOUSE
from app.db.merge_config import get_key_fields

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
        # table_name = f"{database_name}__{table_structure.name}"
        table_name = f"{table_structure.name}"
        
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
            
        # full_table_name = f"{database_name}__{table_name}"
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
            
        # full_table_name = f"{database_name}__{table_name}"
        full_table_name = f"{table_name}"
        
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
            
        # full_table_name = f"{database_name}__{table_name}"
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
            
        # full_table_name = f"{database_name}__{table_name}"
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
        """Вставляет или обновляет данные в таблице ClickHouse используя временные таблицы
        
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
            
        # full_table_name = f"{database_name}__{table_name}"
        full_table_name = f"{table_name}"
        target_table = f"{self.config['database']}.{full_table_name}"
        
        try:
            # Добавляем поле источника к каждой записи
            for row in data:
                row['_source_database'] = database_name
                
            # 1. Создаем временную таблицу с таким же схемой
            temp_table_name = f"temp__{full_table_name}__{uuid.uuid4().hex[:8]}"
            temp_table = f"{self.config['database']}.{temp_table_name}"
            
            # Получаем структуру целевой таблицы
            structure_query = f"DESCRIBE TABLE {target_table}"
            structure_result = self.client.query(structure_query)
            
            # Формируем список полей для создания временной таблицы и собираем имена колонок
            columns_sql = []
            column_names = []
            for row in structure_result.result_rows:
                column_name, column_type = row[0], row[1]
                columns_sql.append(f"`{column_name}` {column_type}")
                column_names.append(column_name)
            
            # Получаем ключевые поля для сравнения записей с проверкой их наличия
            key_fields = get_key_fields(table_name, column_names)
            
            # Проверяем, что все необходимые ключевые поля присутствуют в данных
            for key_field in key_fields:
                if key_field not in column_names:
                    raise ValueError(f"Ключевое поле '{key_field}' отсутствует в таблице '{full_table_name}'")
            
            logger.info(f"Для таблицы '{full_table_name}' используются ключевые поля: {key_fields}")
            
            # Создаем временную таблицу с движком Memory для быстрой обработки
            create_temp_sql = f"""
            CREATE TABLE {temp_table} (
                {','.join(columns_sql)}
            ) ENGINE = Memory
            """
            self.client.command(create_temp_sql)
            logger.debug(f"Создана временная таблица {temp_table}")
            
            # 2. Загружаем данные во временную таблицу
            df = pd.DataFrame(data)
            self.client.insert_df(temp_table, df)
            logger.debug(f"Загружено {len(data)} записей во временную таблицу {temp_table}")
            
            # 3. Вместо DELETE + INSERT используем аналогичный подход с созданием временной таблицы
            # для финального результата, содержащего все строки из основной таблицы, которые не будут заменены + новые данные
            
            # 3.1 Создаем временную таблицу для результата
            result_table_name = f"result__{full_table_name}__{uuid.uuid4().hex[:8]}"
            result_table = f"{self.config['database']}.{result_table_name}"
            create_result_sql = f"""
            CREATE TABLE {result_table} (
                {','.join(columns_sql)}
            ) ENGINE = Memory
            """
            self.client.command(create_result_sql)
            
            # 3.2 Формируем условие для исключения записей, которые будут заменены
            where_conditions = []
            for field in key_fields:
                # Создаем подзапрос для поиска записей из временной таблицы с такими же ключами
                where_conditions.append(f"""
                NOT `{field}` IN (SELECT `{field}` FROM {temp_table} WHERE _source_database = '{database_name}')
                """)
                
            if where_conditions:
                where_condition = " OR ".join(where_conditions)
                # 3.3 Копируем существующие записи, которые не будут заменены
                copy_existing_sql = f"""
                INSERT INTO {result_table} 
                SELECT * FROM {target_table} 
                WHERE (_source_database != '{database_name}') OR ({where_condition})
                """
                self.client.command(copy_existing_sql)
            else:
                # Если нет ключевых полей, просто копируем записи из других источников
                copy_existing_sql = f"""
                INSERT INTO {result_table} 
                SELECT * FROM {target_table} 
                WHERE _source_database != '{database_name}'
                """
                self.client.command(copy_existing_sql)
            
            # 3.4 Добавляем новые записи из временной таблицы
            copy_new_sql = f"""
            INSERT INTO {result_table} SELECT * FROM {temp_table}
            """
            self.client.command(copy_new_sql)
            
            # 3.5 Очищаем основную таблицу
            self.client.command(f"TRUNCATE TABLE {target_table}")
            
            # 3.6 Копируем результат обратно в основную таблицу
            restore_sql = f"""
            INSERT INTO {target_table} SELECT * FROM {result_table}
            """
            self.client.command(restore_sql)
            
            # 4. Удаляем временные таблицы
            self.client.command(f"DROP TABLE IF EXISTS {temp_table}")
            self.client.command(f"DROP TABLE IF EXISTS {result_table}")
            
            logger.info(f"Обновлено/добавлено {len(data)} записей в таблицу '{full_table_name}'")
            return len(data)
        except ClickHouseError as err:
            logger.error(f"Ошибка при обновлении данных в таблице '{full_table_name}': {err}")
            
            # Попробуем удалить временные таблицы, если они остались
            try:
                if 'temp_table' in locals():
                    self.client.command(f"DROP TABLE IF EXISTS {temp_table}")
                if 'result_table' in locals():
                    self.client.command(f"DROP TABLE IF EXISTS {result_table}")
            except Exception:
                pass
                
            raise

    def batch_merge_data(self, database_name: str, table_name: str, data: List[Dict[str, Any]], 
                    batch_size: int = 100000) -> int:
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
        
        # Разбиваем данные на пакеты
        for i in range(0, total_records, batch_size):
            batch = data[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_records + batch_size - 1) // batch_size
            
            logger.info(f"Обработка пакета {batch_num}/{total_batches} ({len(batch)} записей) для {database_name}/{table_name}")
            
            try:
                # Обрабатываем текущий пакет
                processed = self.merge_data(database_name, table_name, batch)
                processed_records += processed
                
                logger.info(f"Пакет {batch_num}/{total_batches} успешно обработан ({processed} записей)")
            except Exception as err:
                logger.error(f"Ошибка обработки пакета {batch_num}/{total_batches}: {err}")
                # Можно добавить механизм повторных попыток здесь
                raise
        
        logger.info(f"Всего обработано {processed_records} записей для {database_name}/{table_name}")
        return processed_records 