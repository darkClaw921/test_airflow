"""
Конфигурация приложения DataStream
"""
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройки для сервисной базы данных MySQL
SERVICE_DB = {
    'host': os.getenv('SERVICE_DB_HOST', 'localhost'),
    'port': int(os.getenv('SERVICE_DB_PORT', '3306')),
    'user': os.getenv('SERVICE_DB_USER', 'root'),
    'password': os.getenv('SERVICE_DB_PASSWORD', ''),
    'database': os.getenv('SERVICE_DB_NAME', 'service_db'),
}

# Настройки для ClickHouse
CLICKHOUSE = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.getenv('CLICKHOUSE_PORT', '8123')),
    'user': os.getenv('CLICKHOUSE_USER', 'default'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
    'database': os.getenv('CLICKHOUSE_DB', 'default'),
}

# Имя сервисной таблицы с настройками подключений
SERVICE_CONNECTIONS_TABLE = os.getenv('SERVICE_CONNECTIONS_TABLE', 'mysql_connections')

# Размер пакета для передачи данных
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000')) 