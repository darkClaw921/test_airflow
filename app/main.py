"""
Точка входа в приложение DataStream: MySQL & ClickHouse
"""
import logging
import sys
import argparse
import json
from typing import List, Dict, Any

from app.services.sync_service import SyncService, SyncResult

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def sync_all() -> List[SyncResult]:
    """Синхронизирует все базы данных MySQL с ClickHouse
    
    Returns:
        Результаты синхронизации
    """
    sync_service = SyncService()
    results = sync_service.sync_all()
    return results


def print_results(results: List[SyncResult]):
    """Выводит результаты синхронизации
    
    Args:
        results: Список результатов синхронизации
    """
    if not results:
        logger.info("Нет результатов для отображения")
        return
        
    logger.info("===== Результаты синхронизации =====")
    
    for result in results:
        status_emoji = "✅" if result.status == "completed" else "❌"
        status_text = f"{status_emoji} {result.status.upper()}"
        
        logger.info(f"{status_text}: {result.connection_name} ({result.database})")
        logger.info(f"  Обработано таблиц: {result.tables_processed}")
        logger.info(f"  Прочитано строк: {result.rows_read}")
        logger.info(f"  Записано строк: {result.rows_written}")
        logger.info(f"  Продолжительность: {result.duration():.2f} секунд")
        
        if result.error:
            logger.error(f"  Ошибка: {result.error}")
            
        logger.info("-" * 30)
        
    total_tables = sum(r.tables_processed for r in results)
    total_rows_read = sum(r.rows_read for r in results)
    total_rows_written = sum(r.rows_written for r in results)
    
    logger.info(f"Всего обработано:")
    logger.info(f"  Подключений: {len(results)}")
    logger.info(f"  Таблиц: {total_tables}")
    logger.info(f"  Строк прочитано: {total_rows_read}")
    logger.info(f"  Строк записано: {total_rows_written}")


def results_to_json(results: List[SyncResult]) -> str:
    """Преобразует результаты в JSON строку
    
    Args:
        results: Список результатов синхронизации
        
    Returns:
        JSON строка с результатами
    """
    return json.dumps([r.to_dict() for r in results], indent=2)


def main():
    """Основная функция приложения"""
    parser = argparse.ArgumentParser(description='Синхронизация данных из MySQL в ClickHouse')
    parser.add_argument('--json', action='store_true', help='Вывод результатов в формате JSON')
    parser.add_argument('--output', help='Путь к файлу для сохранения результатов')
    args = parser.parse_args()
    
    logger.info("Запуск синхронизации данных из MySQL в ClickHouse")
    
    try:
        results = sync_all()
        
        if args.json:
            json_output = results_to_json(results)
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(json_output)
                logger.info(f"Результаты сохранены в файл: {args.output}")
            else:
                print(json_output)
        else:
            print_results(results)
            
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump([r.to_dict() for r in results], f, indent=2)
                logger.info(f"Результаты сохранены в файл: {args.output}")
                
    except Exception as e:
        logger.error(f"Ошибка при выполнении синхронизации: {e}")
        sys.exit(1)
        
    logger.info("Синхронизация завершена")


if __name__ == "__main__":
    main() 