"""
Конфигурация для слияния данных из MySQL в ClickHouse
Определяет ключевые поля для сопоставления записей в таблицах

Эти поля используются для:
1. ORDER BY в таблицах ReplacingMergeTree 
2. Идентификации уникальных записей при автоматическом слиянии
3. Определения какие записи должны заменяться при обновлении
"""
from typing import Dict, List, Optional
from loguru import logger

# Словарь с ключевыми полями для каждой таблицы
# По этим полям будет определяться уникальность записи при обновлении
# В ReplacingMergeTree эти поля используются в ORDER BY для группировки записей
# Формат: 'table_name': ['field1', 'field2', ...]
MERGE_CONFIG: Dict[str, List[str]] = {
    'analytics_orders': ['id'],
    'analytics_services': ['id'],
    'avia_coupons': ['id'],
    'analytics_flights': ['id'],
    'analytics_clients': ['id'],
    'analytics_avia_ticket_service_data': ['id'],
    'analytics_service_flight_relations': ['service_id'],
    'analytics_order_legs': ['order_id'],
    'analytics_service_data': ['service_id'],
    'analytics_emd_service_data': ['id'],
    'analytics_promo_code_usages': ['promo_code_id'],
    'analytics_flight_provider_data': ['id'],
    
}

# Запасные поля для проверки в качестве первичных ключей
# Используются если ключевые поля не определены или отсутствуют в таблице
FALLBACK_KEY_FIELDS = ['id',]

def get_key_fields(table_name: str, available_columns: Optional[List[str]] = None) -> List[str]:
    """
    Получает список ключевых полей для указанной таблицы
    
    Эти поля будут использованы в ORDER BY для ReplacingMergeTree таблицы.
    ReplacingMergeTree группирует записи по этим полям и оставляет только
    запись с максимальной версией (_version) для каждой группы.
    
    Args:
        table_name: Имя таблицы
        available_columns: Список доступных колонок для проверки наличия ключей
        
    Returns:
        Список ключевых полей для ORDER BY
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