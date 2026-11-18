CREATE TABLE IF NOT EXISTS ml.dim_client
(
    client_key     String COMMENT 'Хеш SHA-256 с секретной солью',
    city           String,
    rooms_count    UInt16,
    connected_date Date
)
ENGINE = MergeTree ORDER BY client_key
COMMENT 'Измерение: клиенты без ПДн';

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
COMMENT 'Факт: активность клиента × месяц, client_key — хеш';
