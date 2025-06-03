"""
Модели для работы с сервисной таблицей MySQL
"""
from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class MySQLConnection:
    """Модель для хранения данных о подключении к MySQL"""
    id: int
    name: str
    host: str
    port: int
    user: str
    password: str
    database: str
    enabled: bool = True
    description: Optional[str] = None
    tables: Optional[List[str]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MySQLConnection':
        """Создает объект подключения из словаря"""
        # Если таблицы указаны в виде строки с разделителями, преобразуем их в список
        tables = data.get('tables')
        if isinstance(tables, str):
            tables = [t.strip() for t in tables.split(',') if t.strip()]
        
        return cls(
            id=data['id'],
            name=data['name'],
            host=data['host'],
            port=int(data['port']),
            user=data['user'],
            password=data['password'],
            database=data['database'],
            enabled=bool(data.get('enabled', True)),
            description=data.get('description'),
            tables=tables
        )


@dataclass
class TableStructure:
    """Модель для хранения структуры таблицы MySQL"""
    name: str
    columns: List[Dict[str, Any]]
    primary_key: Optional[List[str]] = None

    @classmethod
    def from_desc(cls, table_name: str, columns_desc: List[Dict[str, Any]]) -> 'TableStructure':
        """Создает объект структуры таблицы из описания столбцов"""
        columns = []
        primary_keys = []
        
        for col in columns_desc:
            column_info = {
                'name': col['Field'],
                'type': col['Type'],
                'nullable': col['Null'] == 'YES',
            }
            columns.append(column_info)
            
            # Определяем первичный ключ
            if col.get('Key') == 'PRI':
                primary_keys.append(col['Field'])
                
        return cls(
            name=table_name,
            columns=columns,
            primary_key=primary_keys if primary_keys else None
        ) 