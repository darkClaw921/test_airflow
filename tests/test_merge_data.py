"""
Тесты для проверки функциональности обновления данных в ClickHouse
"""
import os
import sys
import unittest
from typing import List, Dict, Any
from unittest.mock import MagicMock, patch

# Добавляем путь к основному приложению
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from app.db.clickhouse import ClickHouseClient
from app.db.merge_config import get_key_fields


class TestMergeData(unittest.TestCase):
    """Тесты для функции merge_data в ClickHouseClient"""
    
    def setUp(self):
        """Настройка перед тестами"""
        # Создаем моки для клиента и запросов
        self.mock_client = MagicMock()
        self.mock_query_result = MagicMock()
        
        # Подготавливаем тестовые данные
        self.test_data = [
            {"customer_id": 1, "name": "Иван", "email": "ivan@example.com"},
            {"customer_id": 2, "name": "Мария", "email": "maria@example.com"}
        ]
        
        # Устанавливаем результат структуры таблицы для мока
        self.mock_query_result.result_rows = [
            ["customer_id", "Int32"],
            ["name", "String"],
            ["email", "String"],
            ["_source_database", "String"]
        ]
        
    @patch('app.db.clickhouse.ClickHouseClient.connect')
    @patch('app.db.clickhouse.ClickHouseClient.disconnect')
    def test_batch_merge_data_empty_data(self, mock_disconnect, mock_connect):
        """Тестирует обработку пустого набора данных"""
        client = ClickHouseClient()
        client.client = self.mock_client
        
        result = client.batch_merge_data("test_db", "customers", [])
        
        self.assertEqual(result, 0, "Результат должен быть 0 для пустого набора данных")
        
    @patch('app.db.clickhouse.ClickHouseClient.merge_data')
    @patch('app.db.clickhouse.ClickHouseClient.connect')
    @patch('app.db.clickhouse.ClickHouseClient.disconnect')
    def test_batch_merge_data_batching(self, mock_disconnect, mock_connect, mock_merge_data):
        """Тестирует разделение данных на пакеты"""
        client = ClickHouseClient()
        client.client = self.mock_client
        
        # Создаем большой набор данных
        large_data = []
        for i in range(250000):
            large_data.append({"customer_id": i, "name": f"User {i}"})
        
        mock_merge_data.return_value = 50000  # Каждый пакет возвращает 50000
        
        result = client.batch_merge_data("test_db", "customers", large_data, batch_size=50000)
        
        # Должно быть 5 вызовов merge_data (250000 / 50000 = 5)
        self.assertEqual(mock_merge_data.call_count, 5)
        self.assertEqual(result, 250000)
        
    def test_get_key_fields(self):
        """Тестирует получение ключевых полей для таблицы"""
        # Тестируем существующую таблицу
        customers_keys = get_key_fields("customers")
        self.assertEqual(customers_keys, ["customer_id"])
        
        # Тестируем составной ключ
        order_items_keys = get_key_fields("order_items")
        self.assertEqual(set(order_items_keys), {"order_id", "product_id"})
        
        # Тестируем несуществующую таблицу (должны получить значение по умолчанию)
        unknown_keys = get_key_fields("unknown_table")
        self.assertEqual(unknown_keys, ["id"])
        

if __name__ == "__main__":
    unittest.main() 