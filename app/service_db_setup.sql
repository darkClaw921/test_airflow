-- Скрипт для создания сервисной таблицы в MySQL
-- Эта таблица содержит информацию о подключениях к базам данных MySQL


-- Создаем таблицу для хранения подключений
CREATE TABLE IF NOT EXISTS mysql_connections (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL COMMENT 'Имя подключения',
    host VARCHAR(255) NOT NULL COMMENT 'Хост сервера MySQL',
    port INT NOT NULL DEFAULT 3306 COMMENT 'Порт сервера MySQL',
    user VARCHAR(255) NOT NULL COMMENT 'Имя пользователя',
    password VARCHAR(255) NOT NULL COMMENT 'Пароль пользователя',
    `database` VARCHAR(255) NOT NULL COMMENT 'Имя базы данных',
    enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT 'Активно ли подключение (1 - да, 0 - нет)',
    tables TEXT COMMENT 'Список таблиц для синхронизации (через запятую, если пусто - все таблицы)',
    description TEXT COMMENT 'Описание подключения',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Время создания записи',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Время последнего обновления',
    UNIQUE KEY uk_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Таблица с настройками подключений к MySQL';

-- Добавляем пример подключения
INSERT INTO mysql_connections 
    (name, host, port, user, password, `database`, enabled, tables, description)
VALUES
    ('example_db', 'localhost', 3306, 'example_user', 'example_password', 'example_db', 1, '', 'Пример подключения к базе данных')
ON DUPLICATE KEY UPDATE
    host = VALUES(host),
    port = VALUES(port),
    user = VALUES(user),
    password = VALUES(password),
    `database` = VALUES(`database`),
    enabled = VALUES(enabled),
    tables = VALUES(tables),
    description = VALUES(description); 