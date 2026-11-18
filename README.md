# Обезличивание персональных данных между слоями ClickHouse

Учебный демо-проект. Слой `dwh` содержит звезду с персональными данными клиентов
(синтетика, `synthetic/generate.py`). DAG Airflow `mask_dwh_to_ml` копирует её в слой `ml`
без ПДн:

- из `dim_client` не переносятся `contact_name`, `email`, `phone`, `inn`, `hotel_name`;
  остаются `city`, `rooms_count`, `connected_date`;
- `client_key` в `dim_client` и `fact_client_activity` заменяется хешем
  `lower(hex(substring(SHA256(concat(соль, ':', toString(client_key))), 1, 8)))`.
  Хеш одинаковый в обеих таблицах, поэтому JOIN в `ml` работает;
- `dim_tariff`, `dim_date` и показатели факта копируются без изменений;
- каждый запуск полностью перезаливает `ml` (TRUNCATE + INSERT).

Стек: ClickHouse 26.8, Airflow 3.4.0 (`airflow standalone`), Postgres 17 для метаданных Airflow.

## Соль

- Хранится в Airflow Variable `masking_salt`, зашифрована ключом `FERNET_KEY` из `.env`.
  Если `MASKING_SALT` в `.env` пустой, соль генерируется при первом старте (`masking/init_salt.py`).
  Если сохранённая соль не расшифровывается текущим `FERNET_KEY`, Airflow не стартует, чтобы не создать новую соль.
- Соль не короче 16 символов (`check_salt`).
- В ClickHouse соль передаётся параметром `{salt:String}`, а не склейкой строк. Для таких запросов отключён
  `system.query_log` (`masking/db.py`).
- `FERNET_KEY` в `.env.example` демонстрационный. Для чего-либо кроме демо сгенерируйте свой:
  `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.

## Доступ

- `etl_user` (Airflow) имеет все права на `dwh` и `ml`.
- `ml_user` имеет только `SELECT` на `ml.*` и к `dwh` доступа не имеет.

Пользователи и права создаются при первом старте ClickHouse (`clickhouse/initdb/01_users.sh`).
На старом volume изменения применятся только после `docker compose down -v`.

## Запуск

```bash
cp .env.example .env
```

```bash
docker compose up -d
```

Через 1–2 минуты откройте Airflow http://localhost:8080 (вход без пароля),
DAG `mask_dwh_to_ml` → **Trigger**. Задачи: `generate_dwh >> mask_to_ml`. В логе `mask_to_ml`
выводится число строк каждой таблицы в `dwh` и `ml`.

Остановить и удалить данные: `docker compose down -v`.

## Ручная проверка

ClickHouse Play: http://localhost:8123/play (`etl_user` / `etl_pass`). Запросы выполняются по одному.

1. В `ml.dim_client` нет колонок с ПДн:

```sql
SELECT name FROM system.columns WHERE database = 'ml' AND table = 'dim_client';
```

2. Число строк в `dwh` и `ml` совпадает по каждой таблице:

```sql
SELECT 'dim_client' AS t, (SELECT count() FROM dwh.dim_client) AS dwh, (SELECT count() FROM ml.dim_client) AS ml
UNION ALL SELECT 'dim_tariff', (SELECT count() FROM dwh.dim_tariff), (SELECT count() FROM ml.dim_tariff)
UNION ALL SELECT 'dim_date', (SELECT count() FROM dwh.dim_date), (SELECT count() FROM ml.dim_date)
UNION ALL SELECT 'fact_client_activity', (SELECT count() FROM dwh.fact_client_activity), (SELECT count() FROM ml.fact_client_activity);
```

3. JOIN факта с `dim_client` в `ml` не теряет строк:

```sql
SELECT (SELECT count() FROM ml.fact_client_activity) AS fact_rows,
       (SELECT count() FROM ml.fact_client_activity AS f INNER JOIN ml.dim_client AS c ON f.client_key = c.client_key) AS joined_rows;
```

## Структура

```
docker-compose.yml        postgres + clickhouse + airflow
.env.example              пароли, FERNET_KEY, MASKING_SALT
clickhouse/initdb/        базы dwh, ml и пользователи etl_user, ml_user
clickhouse/config/        уровень серверного лога information
dags/mask_dwh_to_ml.py    DAG: generate_dwh >> mask_to_ml
synthetic/                DDL слоя dwh и генератор синтетики
masking/
  db.py                   clickhouse-connect, выполнение SQL, отключение query_log для запросов с солью
  init_salt.py            создание или проверка Variable masking_salt при старте Airflow
  masker.py               check_salt, mask_to_ml(salt)
  sql/ddl_ml.sql          таблицы слоя ml
  sql/mask.sql            заливка четырёх таблиц ml
```

Предыдущая версия проекта (k-анонимность, маски, проверки, отчёт, эксперимент) сохранена под тегом `v0-full`.
