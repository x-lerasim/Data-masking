-- Слой 2 (ml): обезличенная звезда. Таблицы создаются один раз,
-- данные полностью перезаливаются (TRUNCATE + INSERT) при каждом запуске DAG.
-- masking_log не очищается: это журнал запусков.

CREATE TABLE IF NOT EXISTS ml.dim_client
(
    client_key        String COMMENT 'Псевдоним: SHA-256 с секретной солью',
    email_masked      String,
    phone_masked      String,
    federal_district  LowCardinality(String),
    rooms_range       LowCardinality(String),
    connected_quarter LowCardinality(String)
)
ENGINE = MergeTree ORDER BY client_key
COMMENT 'Измерение: клиенты, обезличено';

CREATE TABLE IF NOT EXISTS ml.dim_tariff
(
    tariff_key  UInt8,
    tariff_name String,
    price       Decimal(10, 2)
)
ENGINE = MergeTree ORDER BY tariff_key;

CREATE TABLE IF NOT EXISTS ml.dim_date
(
    date_key Date,
    year     UInt16,
    quarter  UInt8,
    month    UInt8
)
ENGINE = MergeTree ORDER BY date_key;

CREATE TABLE IF NOT EXISTS ml.fact_client_activity
(
    client_key         String,
    tariff_key         UInt8,
    date_key           Date,
    logins_count       UInt32,
    active_users       UInt16,
    bookings_processed UInt32,
    features_used      UInt8,
    tickets_count      UInt16,
    late_payments      UInt8,
    mrr                Decimal(12, 2),
    churn_flag         UInt8
)
ENGINE = MergeTree ORDER BY (client_key, date_key)
COMMENT 'Факт: активность клиента × месяц, client_key — псевдоним';

CREATE TABLE IF NOT EXISTS ml.masking_log
(
    run_id                String,
    logged_at             DateTime DEFAULT now(),
    dim_client_read       UInt32,
    dim_client_written    UInt32,
    dim_tariff_read       UInt32,
    dim_tariff_written    UInt32,
    dim_date_read         UInt32,
    dim_date_written      UInt32,
    fact_read             UInt32,
    fact_written          UInt32,
    suppressed_clients    UInt32,
    suppressed_fact_rows  UInt32,
    k_min                 UInt32,
    k_actual              UInt32,
    checks_status         LowCardinality(String) COMMENT 'PASSED / FAILED',
    checks_details        String
)
ENGINE = MergeTree ORDER BY logged_at
COMMENT 'Журнал запусков маскирования';

-- Источник для ML: без email_masked и phone_masked
CREATE OR REPLACE VIEW ml.v_ml_client_month AS
SELECT
    f.client_key AS client_key,
    f.date_key AS date_key,
    d.year AS year,
    d.quarter AS quarter,
    d.month AS month,
    c.federal_district AS federal_district,
    c.rooms_range AS rooms_range,
    c.connected_quarter AS connected_quarter,
    f.tariff_key AS tariff_key,
    t.tariff_name AS tariff_name,
    t.price AS tariff_price,
    f.logins_count AS logins_count,
    f.active_users AS active_users,
    f.bookings_processed AS bookings_processed,
    f.features_used AS features_used,
    f.tickets_count AS tickets_count,
    f.late_payments AS late_payments,
    f.mrr AS mrr,
    f.churn_flag AS churn_flag
FROM ml.fact_client_activity AS f
INNER JOIN ml.dim_client AS c ON f.client_key = c.client_key
INNER JOIN ml.dim_tariff AS t ON f.tariff_key = t.tariff_key
INNER JOIN ml.dim_date   AS d ON f.date_key   = d.date_key;
