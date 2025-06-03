-- Создание базы данных для аналитики
CREATE DATABASE IF NOT EXISTS analytics;

-- Использование базы данных
USE analytics;

-- Таблица клиентов
CREATE TABLE IF NOT EXISTS customers (
    customer_id UInt32,
    first_name String,
    last_name String,
    email String,
    phone String,
    registration_date DateTime,
    status String,
    city String,
    country String
) ENGINE = MergeTree()
ORDER BY (customer_id);

-- Таблица продуктов
CREATE TABLE IF NOT EXISTS products (
    product_id UInt32,
    name String,
    category String,
    price Decimal(10, 2),
    description String,
    stock_quantity UInt32,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (product_id);

-- Таблица заказов с поддержкой аналитических запросов
CREATE TABLE IF NOT EXISTS orders (
    order_id UInt32,
    customer_id UInt32,
    order_date DateTime,
    total_amount Decimal(12, 2),
    status String,
    shipping_address String,
    payment_method String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_id, customer_id, order_date);

-- Таблица деталей заказа
CREATE TABLE IF NOT EXISTS order_items (
    item_id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity UInt32,
    unit_price Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (order_id, product_id);

-- Материализованное представление для анализа продаж по категориям
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_category_sales
ENGINE = SummingMergeTree()
PARTITION BY category
ORDER BY (category)
AS SELECT
    p.category,
    count() AS total_items_sold,
    sum(oi.quantity) AS total_quantity,
    sum(oi.quantity * oi.unit_price) AS total_revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status != 'cancelled'
GROUP BY p.category;

-- Материализованное представление для анализа продаж по дням
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day)
AS SELECT
    toDate(o.order_date) AS day,
    count() AS order_count,
    sum(o.total_amount) AS total_revenue,
    count(DISTINCT o.customer_id) AS unique_customers
FROM orders o
WHERE o.status != 'cancelled'
GROUP BY day;

-- Материализованное представление для анализа активности клиентов
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_activity
ENGINE = SummingMergeTree()
ORDER BY (customer_id)
AS SELECT
    c.customer_id,
    c.city,
    count() AS order_count,
    sum(o.total_amount) AS total_spent,
    max(o.order_date) AS last_order_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.city;

-- Материализованное представление для анализа популярности продуктов
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_popularity
ENGINE = SummingMergeTree()
ORDER BY (product_id)
AS SELECT
    p.product_id,
    p.name,
    p.category,
    count() AS times_ordered,
    sum(oi.quantity) AS total_quantity_sold,
    sum(oi.quantity * oi.unit_price) AS total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status != 'cancelled'
GROUP BY p.product_id, p.name, p.category; 