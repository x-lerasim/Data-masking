# Обезличивание персональных данных между слоями ClickHouse

Учебный демо-проект. Слой `dwh` содержит звезду с персональными данными клиентов
(синтетика, `synthetic/generate.py`). Модели dbt строят из неё слой `ml` без ПДн:

- из `dim_client` не переносятся `contact_name`, `email`, `phone`, `inn`, `hotel_name`;
  остаются `city`, `rooms_count`, `connected_date`;
- `client_key` в `dim_client` и `fact_client_activity` заменяется хешем
  `lower(hex(substring(SHA256(concat(соль, ':', toString(client_key))), 1, 8)))`
  (макрос `dbt/macros/pseudo_key.sql`). Хеш одинаковый в обеих таблицах, поэтому JOIN в `ml` работает;
- `dim_tariff`, `dim_date` и показатели факта копируются без изменений;
- каждый `dbt run` пересоздаёт таблицы `ml` целиком (`materialized: table`).

Стек: ClickHouse 26.8, Airflow 3.4.0 (`airflow standalone`), dbt-core 1.12 + dbt-clickhouse 1.10.3,
Postgres 17 для метаданных Airflow.

## Как устроен запуск

DAG `mask_dwh_to_ml` в Airflow:

```
generate_dwh  →  mask_to_ml
(Python,         (DockerOperator: контейнер data-masking-dbt, команда dbt run)
 пишет dwh)
```

Образ `data-masking-dbt` собирается из `dbt/Dockerfile` при `docker compose up`. Сервис `dbt`
в compose только собирает образ, печатает версию dbt и завершается. Airflow запускает из этого
образа отдельный контейнер на каждый запуск DAG через `/var/run/docker.sock` и удаляет его после успеха.

## Соль

- Задаётся в `.env` (`MASKING_SALT`), обязательна. В dbt попадает через переменную окружения контейнера
  (`private_environment` в DockerOperator: в базу метаданных Airflow и логи задач не попадает).
- В SQL значение соли не подставляется: `profiles.yml` передаёт её в ClickHouse как пользовательскую
  настройку `SQL_masking_salt`, а модель читает `getSetting('SQL_masking_salt')`.
- Для соединения dbt отключён `system.query_log` (`log_queries: 0`).
- Если соль короче 16 символов, `dbt run` падает с ошибкой компиляции.
- Значение в `.env.example` демонстрационное, для чего-либо кроме демо замените его.
- Смена соли меняет все хеши.

## Доступ

- `etl_user` (Airflow, dbt) имеет все права на `dwh` и `ml`.
- `ml_user` имеет только `SELECT` на `ml.*` и к `dwh` доступа не имеет.

Пользователи и права создаются при первом старте ClickHouse (`clickhouse/initdb/01_users.sh`).
На старом volume изменения применятся только после `docker compose down -v`.

## Запуск

Нужен Docker с Compose v2.

```bash
cp .env.example .env
```

На Linux в `.env` укажите группу docker-сокета, иначе Airflow не сможет запустить контейнер dbt
(в Docker Desktop оставьте `DOCKER_GID=0`):

```bash
getent group docker | cut -d: -f3
```

```bash
docker compose up -d
```

Через 1–2 минуты откройте Airflow http://localhost:8080 (вход без пароля),
DAG `mask_dwh_to_ml` → **Trigger**. В логе `mask_to_ml` вывод `dbt run`: четыре модели `OK created`.

После правок в `dbt/` пересоберите образ:

```bash
docker compose build dbt
```

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
docker-compose.yml        postgres + clickhouse + airflow + сборка образа dbt
.env.example              пароли, MASKING_SALT, DOCKER_GID
clickhouse/initdb/        базы dwh, ml и пользователи etl_user, ml_user
clickhouse/config/        уровень серверного лога information
dags/mask_dwh_to_ml.py    DAG: generate_dwh >> mask_to_ml (DockerOperator с dbt run)
synthetic/                DDL слоя dwh, генератор синтетики, подключение к ClickHouse
dbt/
  Dockerfile              python:3.12-slim + dbt-clickhouse
  dbt_project.yml
  profiles.yml            подключение под etl_user, соль как настройка SQL_masking_salt, log_queries: 0
  macros/pseudo_key.sql   хеш client_key и проверка длины соли
  models/sources.yml      источник dwh
  models/ml/*.sql         dim_client, dim_tariff, dim_date, fact_client_activity
```

Предыдущая версия проекта (k-анонимность, маски, проверки, отчёт, эксперимент) сохранена под тегом `v0-full`.
