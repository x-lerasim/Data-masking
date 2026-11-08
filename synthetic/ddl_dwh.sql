-- Слой 1 (dwh): звезда по Кимбаллу с персональными данными.
-- Полная перезагрузка: таблицы пересоздаются при каждом generate_dwh.

DROP TABLE IF EXISTS dwh.fact_client_activity;
DROP TABLE IF EXISTS dwh.dim_client;
DROP TABLE IF EXISTS dwh.dim_tariff;
DROP TABLE IF EXISTS dwh.dim_date;

CREATE TABLE dwh.dim_client
(
    client_key     UInt32 COMMENT 'Суррогатный ключ клиента',
    contact_name   String COMMENT 'ФИО контактного лица',
    email          String,
    phone          String,
    inn            String COMMENT 'ИНН организации / ИП',
    hotel_name     String,
    city           String,
    rooms_count    UInt16 COMMENT 'Число номеров',
    connected_date Date   COMMENT 'Дата подключения к SaaS'
)
ENGINE = MergeTree ORDER BY client_key
COMMENT 'Измерение: клиенты (отели)';

CREATE TABLE dwh.dim_tariff
(
    tariff_key  UInt8,
    tariff_name String,
    price       Decimal(10, 2) COMMENT 'Цена, руб./мес.'
)
ENGINE = MergeTree ORDER BY tariff_key
COMMENT 'Измерение: тарифы';

CREATE TABLE dwh.dim_date
(
    date_key Date COMMENT 'Первое число месяца',
    year     UInt16,
    quarter  UInt8,
    month    UInt8
)
ENGINE = MergeTree ORDER BY date_key
COMMENT 'Измерение: календарь (месяцы)';

CREATE TABLE dwh.fact_client_activity
(
    client_key         UInt32,
    tariff_key         UInt8,
    date_key           Date,
    logins_count       UInt32,
    active_users       UInt16,
    bookings_processed UInt32,
    features_used      UInt8,
    tickets_count      UInt16,
    late_payments      UInt8,
    mrr                Decimal(12, 2),
    churn_flag         UInt8 COMMENT '1 — клиент ушёл в этом месяце'
)
ENGINE = MergeTree ORDER BY (client_key, date_key)
COMMENT 'Факт: активность клиента, зерно клиент × месяц';
