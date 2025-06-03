"""
Конфигурация для слияния данных из MySQL в ClickHouse
Определяет ключевые поля для сопоставления записей в таблицах
"""
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

# Словарь с ключевыми полями для каждой таблицы
# По этим полям будет определяться уникальность записи при обновлении
# Формат: 'table_name': ['field1', 'field2', ...]
MERGE_CONFIG: Dict[str, List[str]] = {
    'customers': ['customer_id'],
    'orders': ['order_id'],
    'order_items': ['order_id', 'product_id'],
    'products': ['product_id'],
    'categories': ['category_id'],
    'users': ['user_id'],
    'transactions': ['transaction_id'],
    'inventory': ['inventory_id'],
    'suppliers': ['supplier_id'],
    'employees': ['employee_id'],
    # Аналитические таблицы
    'category_sales': ['category'],
    'customer_activity': ['customer_id'],
    # Добавьте другие таблицы при необходимости
}

# Запасные поля для проверки в качестве первичных ключей
FALLBACK_KEY_FIELDS = ['id',]

def get_key_fields(table_name: str, available_columns: Optional[List[str]] = None) -> List[str]:
    """
    Получает список ключевых полей для указанной таблицы
    
    Args:
        table_name: Имя таблицы
        available_columns: Список доступных колонок для проверки наличия ключей
        
    Returns:
        Список ключевых полей
    """
    # Сначала пробуем получить ключи из конфигурации
    key_fields = MERGE_CONFIG.get(table_name)
    
    # Если ключи не определены или нужно проверить их наличие
    if not key_fields or (available_columns is not None):
        if not available_columns:
            # Если колонки не предоставлены, вернем стандартный id
            return ['id']
        
        # Если в конфигурации есть ключи, проверим их наличие
        if key_fields:
            # Проверяем, что все ключевые поля присутствуют в таблице
            missing_fields = [field for field in key_fields if field not in available_columns]
            if missing_fields:
                logger.warning(
                    f"Для таблицы '{table_name}' не найдены ключевые поля: {missing_fields}. "
                    f"Будет использован альтернативный ключ."
                )
                key_fields = None
        
        # Если ключей нет в конфигурации или они отсутствуют в таблице,
        # пробуем найти подходящие поля из запасного списка
        if not key_fields:
            for field in FALLBACK_KEY_FIELDS:
                if field in available_columns:
                    logger.info(f"Для таблицы '{table_name}' используется запасное ключевое поле: {field}")
                    return [field]
            
            # Если ничего не нашли, используем первое поле как ключ
            logger.warning(
                f"Для таблицы '{table_name}' не найдены подходящие ключевые поля. "
                f"Используем первое поле: {available_columns[0]}"
            )
            return [available_columns[0]]
    
    return key_fields or ['id'] 