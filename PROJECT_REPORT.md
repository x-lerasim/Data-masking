# Отчёт по проекту: обезличивание персональных данных между двумя слоями ClickHouse

Предмет: «ИИ для информационной безопасности». Репозиторий `x-lerasim/data-masking`, ветка
`claude/tender-franklin-hlzwmq` (код в состоянии коммита с этим отчётом). Дата запуска: 2026-11-09, время в UTC.

## 0. Как получены цифры

Все цифры ниже взяты из запуска 2026-11-09 на стенде из `docker-compose.yml` после правок, описанных
в разделах 6.3, 7.3 и 8. Если число взято из кода или README, а не из запуска, это сказано рядом с ним.
Если что-то не проверялось, это сказано явно (сводка в 13.3).

Условия запуска:

- Linux, 4 CPU, 15,7 GiB RAM, Docker 29.9.2, Docker Compose v5.6.1;
- `.env` создан копированием `.env.example` (`K_MIN=5`, демонстрационный `FERNET_KEY`, `MASKING_SALT` пустой,
  поэтому соль сгенерирована при первом старте);
- перед запуском volumes пустые, то есть `clickhouse/initdb/01_users.sh` выполнился с новыми правами.

Выполненные запуски DAG `mask_dwh_to_ml`:

| № | run_id | Что проверялось | Итог |
|---|---|---|---|
| 1 | `manual__2026-11-09T12:16:36.326553+00:00` | основной запуск | success, PASSED |
| 2 | `manual__2026-11-09T12:17:28.663423+00:00` | повторная запускаемость | success, PASSED |
| 3 | `manual__2026-11-09T12:18:22.043090+00:00` | после `docker compose up -d --force-recreate airflow` | success, PASSED, псевдонимы те же |
| – | без запуска DAG | старт Airflow с чужим `FERNET_KEY` (раздел 6.3) | контейнер завершился с кодом 1 и сообщением об ошибке |
| 4 | `manual__2026-11-09T12:20:17.955136+00:00` | после возврата прежнего `FERNET_KEY` | success, PASSED, псевдонимы те же |
| – | без run_id, `masker.validate(5)` вручную | негативный тест проверок (раздел 9) | 3 проверки FAIL |
| 5 | `manual__2026-11-09T12:20:44.612581+00:00` | восстановление `ml` после негативного теста | success, PASSED |
| – | `python -m masking.experiment` | эксперимент K_MIN 3/5/10, квартал/полугодие (раздел 7.3) | таблицы в `experiments/` |

Если не сказано иное, цифры относятся к запуску 1. Счётчики запусков 1–5 в `ml.masking_log` одинаковые.

## 1. Цель проекта и что в него не входит

**Цель** (по README и коду): учебный демо-проект. SaaS для отелей хранит витрину активности клиентов
с персональными данными (слой `dwh`). Из неё автоматически строится обезличенная копия (слой `ml`),
из которой модель прогноза оттока берёт данные. Обезличивание сочетает псевдонимизацию ключа с солью,
удаление прямых идентификаторов, маскирование, обобщение квазиидентификаторов и подавление малых групп
(k-анонимность). Результат проверяется автоматически, каждый запуск пишется в журнал. Доступ к слоям
разграничен средствами RBAC ClickHouse.

**Чего в проекте нет** (проверено по коду):

- **Модели оттока нет.** Нет кода обучения, ML-библиотек и признаков сверх витрины `ml.v_ml_client_month`.
  Проект только готовит для неё данные.
- **Реальных данных нет.** Все данные синтетические (`synthetic/generate.py`), при каждом запуске DAG
  слой `dwh` удаляется и создаётся заново.
- Нет инкрементальной загрузки: каждый раз полная перезаливка (TRUNCATE + INSERT).
- Нет TLS между компонентами: ClickHouse отвечает по HTTP на 8123.
- Нет аутентификации в Airflow: `SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`, вход без пароля.
- Нет ротации соли и внешнего хранилища секретов (Vault и т. п.).
- Нет метрик l-разнообразия и t-близости, нет оценки риска по атрибутам факта.
- Нет обезличивания по требованию госорганов (постановление № 1154), нет передачи данных третьим лицам.
- Нет автотестов (pytest и т. п.) и CI. Проверки есть только в задаче `validate_ml`.
- Нет локальных актов оператора (приказа об обезличивании, правил, перечней).
- В коде нет комментариев: описание лежит в README.

## 2. Стек, файлы, ресурсы

### 2.1 Версии (фактически запущенные)

| Компонент | Задано в коде | Фактически в запуске |
|---|---|---|
| ClickHouse | образ `clickhouse/clickhouse-server:26.8` | `SELECT version()` → **26.8.36.3** |
| Airflow | образ `apache/airflow:3.4.0-python3.12` | **3.4.0** (`/api/v2/version`), LocalExecutor, `airflow standalone` |
| Python (в контейнере Airflow) | python3.12 | **3.12.16** |
| PostgreSQL (метаданные Airflow) | `postgres:17-alpine` | **17.11** |
| clickhouse-connect | `_PIP_ADDITIONAL_REQUIREMENTS: clickhouse-connect==1.12.0` | **1.12.0** (+ lz4 4.4.5, backports.zstd 1.8.0) |
| cryptography (Fernet) | не задан, из образа Airflow | 50.0.2 |
| PyYAML | не задан, из образа Airflow | 6.0.3 |
| pendulum | не задан, из образа Airflow | 3.3.0 |
| psycopg2-binary | не задан, из образа Airflow | 2.9.13 |
| apache-airflow-providers-postgres | не задан, из образа Airflow | 7.1.1 |

Контейнеров три: `postgres`, `clickhouse`, `airflow`.

### 2.2 Дерево файлов

```
.env.example                  образец .env: K_MIN, пароли etl_user/ml_user/admin ClickHouse, FERNET_KEY, MASKING_SALT
.gitignore                    исключает .env, __pycache__, *.pyc, logs/
README.md                     описание проекта, схемы, правила, права, эксперимент, сценарий показа
docker-compose.yml            три сервиса; переменные из .env с ${VAR:?} (без .env compose не стартует);
                              AIRFLOW__CORE__FERNET_KEY из .env; при старте airflow: db migrate,
                              python -m masking.init_salt, airflow standalone
checks.sql                    запросы для показа в ClickHouse Play (разделы 1–4)
clickhouse/initdb/01_users.sh при первом старте ClickHouse создаёт базы dwh, ml и пользователей etl_user, ml_user с правами
clickhouse/config/logging.xml уровень серверного лога ClickHouse = information
dags/mask_dwh_to_ml.py        DAG из 6 задач; только вызывает функции модулей, читает Variables masking_salt и k_min
synthetic/__init__.py         пустой
synthetic/ddl_dwh.sql         DROP + CREATE четырёх таблиц слоя dwh
synthetic/generate.py         детерминированный генератор синтетики (seed 20251101), вставка, проверки ссылочной целостности и зерна
masking/__init__.py           пустой
masking/rules.yaml            правила по полям, квазиидентификаторы, диапазоны номеров, справочник город → федеральный округ
masking/db.py                 подключение clickhouse-connect под etl_user; разбор SQL-файлов; отключение query_log для запросов с солью
masking/masker.py             формула псевдонима, подстановка шаблонов в SQL, сверка схемы dwh с rules.yaml, статистика, validate, журнал
masking/init_salt.py          при старте Airflow: создать соль, взять сохранённую или упасть, если она не расшифровывается
masking/experiment.py         эксперимент K_MIN 3/5/10 × квартал/полугодие; вывод в md или csv
masking/sql/ddl_ml.sql        CREATE IF NOT EXISTS таблиц ml, masking_log; CREATE OR REPLACE VIEW v_ml_client_month (SQL SECURITY DEFINER)
masking/sql/dwh_client_qi.sql обобщение квазиидентификаторов и размер группы (оконная функция); общий фрагмент для маскирования и проверок
masking/sql/mask_dim_client.sql  TRUNCATE + INSERT ml.dim_client с псевдонимом, масками, обобщением, фильтром k
masking/sql/copy_dims.sql     TRUNCATE + INSERT ml.dim_tariff, ml.dim_date (копия как есть)
masking/sql/mask_fact.sql     TRUNCATE + INSERT ml.fact_client_activity с псевдонимом, без строк подавленных клиентов
masking/sql/validate_ml.sql   восемь проверок; каждая возвращает ok и details
masking/sql/experiment_k.sql  клиенты dwh с КИ (квартал и полугодие), числом строк факта и строк оттока
experiments/k_min_experiment.md   результат эксперимента, таблицы
experiments/k_min_experiment.csv  то же в длинном формате (scenario, metric, value)
```

Без этого отчёта в репозитории 1676 строк (`wc -l` по всем файлам).

### 2.3 Требования к памяти и диску

В репозитории требования к памяти **не указаны**: лимитов `mem_limit` в compose нет. Замер `docker stats`
после запусков DAG:

| Контейнер | RAM |
|---|---|
| airflow (standalone: api-server, scheduler, dag-processor, triggerer) | 1,49 GiB |
| clickhouse | 249 MiB |
| postgres | 65 MiB |
| **Итого** | ≈ 1,8 GiB |

Образы на диске: airflow 3,25 GB, clickhouse 1,2 GB, postgres 424 MB, итого ≈ 4,9 GB. Данные в ClickHouse
(`system.parts`): `dwh` 291,65 KiB, `ml` 214,53 KiB. Минимальный объём памяти не проверялся.

## 3. Архитектура

### 3.1 Слои и базы

Обе базы находятся в одном экземпляре ClickHouse, базы и пользователей создаёт `clickhouse/initdb/01_users.sh`:

| База | Комментарий в DDL | Содержимое | Кто имеет доступ |
|---|---|---|---|
| `dwh` | «Слой 1: витрина с персональными данными» | звезда с ПДн | `etl_user` (все права) |
| `ml` | «Слой 2: обезличенная копия для ML» | обезличенная звезда, журнал, витрина | `etl_user` (все права), `ml_user` (SELECT только `v_ml_client_month` и `masking_log`) |

Метаданные Airflow, включая Variable `masking_salt`, хранятся в отдельном Postgres (volume `pg_data`).

### 3.2 Таблицы

`dwh`: `dim_client`, `dim_tariff`, `dim_date`, `fact_client_activity`.
`ml`: те же четыре таблицы (в `dim_client` другой набор колонок), плюс `masking_log` и представление `v_ml_client_month`.

### 3.3 Поток данных

```
Airflow DAG mask_dwh_to_ml (ручной запуск, schedule=None, max_active_runs=1)

generate_dwh ─► mask_dim_client ─► copy_dims ─► mask_fact ─► validate_ml ─► write_log (all_done)
     │                │                │             │             │              │
     ▼                ▼                ▼             ▼             ▼              ▼
 dwh.* (DROP/     ml.dim_client   ml.dim_tariff  ml.fact_      читает dwh и ml  ml.masking_log
 CREATE+INSERT)   (TRUNCATE+      ml.dim_date    client_activity               (INSERT, не очищается)
                  INSERT ... SELECT FROM dwh)    (TRUNCATE+INSERT ... SELECT FROM dwh)
```

Данные из `dwh` в `ml` переносятся SQL-запросами `INSERT ... SELECT` внутри ClickHouse. Python персональные
данные не обрабатывает, кроме генерации синтетики. Все соединения из Airflow идут под `etl_user`
(`masking/db.py`).

### 3.4 Связь с ВКР

Источник данных для модели оттока: представление `ml.v_ml_client_month` (`masking/sql/ddl_ml.sql`).
Это INNER JOIN факта с тремя измерениями слоя `ml`, одна строка на пару «клиент-псевдоним × месяц».
Колонки: `client_key` (псевдоним), `date_key`, `year`, `quarter`, `month`, `federal_district`,
`rooms_range`, `connected_quarter`, `tariff_key`, `tariff_name`, `tariff_price`, `logins_count`,
`active_users`, `bookings_processed`, `features_used`, `tickets_count`, `late_payments`, `mrr`,
`churn_flag` (целевая переменная). Масок `email_masked` и `phone_masked` в представлении нет.

Модель читает данные под `ml_user`, у которого есть только `SELECT` на `ml.v_ml_client_month` и `ml.masking_log`.
Таблицы `ml.dim_client` (с масками) и `ml.fact_client_activity` ему недоступны (раздел 8). Самой модели
и кода, который читает витрину, в репозитории нет.

## 4. Схема слоёв и синтетика

### 4.1 Слой `dwh` (`synthetic/ddl_dwh.sql`)

Все таблицы на движке `MergeTree`.

**dwh.dim_client**, `ORDER BY client_key`:

| Поле | Тип | Комментарий в DDL |
|---|---|---|
| client_key | UInt32 | Суррогатный ключ клиента |
| contact_name | String | ФИО контактного лица |
| email | String | |
| phone | String | |
| inn | String | ИНН организации / ИП |
| hotel_name | String | |
| city | String | |
| rooms_count | UInt16 | Число номеров |
| connected_date | Date | Дата подключения к SaaS |

**dwh.dim_tariff**, `ORDER BY tariff_key`: `tariff_key UInt8`, `tariff_name String`, `price Decimal(10,2)` (руб./мес.).

**dwh.dim_date**, `ORDER BY date_key`: `date_key Date` (первое число месяца), `year UInt16`, `quarter UInt8`, `month UInt8`.

**dwh.fact_client_activity**, `ORDER BY (client_key, date_key)`. Зерно: **клиент × месяц**.

| Поле | Тип |
|---|---|
| client_key | UInt32 (FK → dim_client) |
| tariff_key | UInt8 (FK → dim_tariff) |
| date_key | Date (FK → dim_date) |
| logins_count | UInt32 |
| active_users | UInt16 |
| bookings_processed | UInt32 |
| features_used | UInt8 |
| tickets_count | UInt16 |
| late_payments | UInt8 |
| mrr | Decimal(12,2) |
| churn_flag | UInt8 (1 — клиент ушёл в этом месяце) |

ClickHouse не поддерживает PK/FK как ограничения. Ключи здесь логические: `ORDER BY` задаёт только сортировку
и не гарантирует уникальность. Ссылочную целостность и зерно проверяет `generate()` после вставки:
строк без пары в измерениях должно быть 0, `count() - uniqExact(client_key, date_key)` тоже 0.

### 4.2 Слой `ml` (`masking/sql/ddl_ml.sql`)

**ml.dim_client**, `ORDER BY client_key`: `client_key String` (псевдоним), `email_masked String`,
`phone_masked String`, `federal_district LowCardinality(String)`, `rooms_range LowCardinality(String)`,
`connected_quarter LowCardinality(String)`. На схеме в README тип указан как `String(16)`, в DDL просто
`String`; фактическая длина псевдонима во всех строках 16 символов.

**ml.dim_tariff** и **ml.dim_date** по типам совпадают с `dwh`.

**ml.fact_client_activity** совпадает с `dwh`, кроме `client_key String` (псевдоним).
`ORDER BY (client_key, date_key)`.

**ml.masking_log**, `ORDER BY logged_at`: `run_id String`, `logged_at DateTime DEFAULT now()`,
`dim_client_read/written`, `dim_tariff_read/written`, `dim_date_read/written`, `fact_read/written`,
`suppressed_clients`, `suppressed_fact_rows`, `k_min`, `k_actual` (все UInt32),
`checks_status LowCardinality(String)` (PASSED/FAILED), `checks_details String` (JSON).

**ml.v_ml_client_month**: представление (VIEW) с `DEFINER = CURRENT_USER SQL SECURITY DEFINER`.
Создаётся задачей DAG под `etl_user`, поэтому `SHOW CREATE` показывает `DEFINER = etl_user SQL SECURITY DEFINER`.

### 4.3 Генерация синтетики (`synthetic/generate.py`)

Параметры из кода:

- `SEED = 20251101` (`random.Random`), генерация детерминирована;
- `N_CLIENTS = 1000`; окно с `WINDOW_START = 2024-11-01` на `N_MONTHS = 24` месяца (2024-11 … 2026-10);
- 26 городов с весами (Москва 20, Санкт-Петербург 12, Сочи 9, Казань 5, …, Мурманск, Хабаровск,
  Кисловодск, Пятигорск по 1);
- ФИО составляются из 12 мужских и 12 женских пар «имя + отчество» и 28 фамилий (у женщин с окончанием «-а»);
- email: `транслит(имя).транслит(фамилия)N@домен`, 6 доменов, уникальность обеспечена;
- телефон: `+7 (9XX) XXX-XX-XX`, уникален;
- ИНН: с вероятностью 0,35 двенадцатизначный (ИП), иначе десятизначный, с корректными контрольными цифрами; уникален;
- название отеля: «тип «название»», 7 типов × 20 названий;
- число номеров: диапазоны 4–20 / 21–50 / 51–100 / 101–450 с весами 42/33/17/8;
- дата подключения: один из 6 кварталов 2024-Q1 … 2025-Q2 (веса 20/20/18/16/14/12);
- 4 тарифа: Старт 4900, Бизнес 9900, Премиум 19900, Сеть 39900. Начальный тариф зависит от числа номеров,
  каждый месяц с вероятностью 0,02 тариф повышается;
- факт: «здоровье» клиента `health ~ Beta(5,2)`, накопительный спад `decline`. Вероятность ухода в месяц:
  `0.004 + 0.05·(1−health)² + 0.03·decline`. После месяца с `churn_flag = 1` строки клиента прекращаются.
  Показатели выводятся из `health`, `decline` и числа номеров со случайным множителем. `mrr` равен цене
  текущего тарифа. Ряд клиента начинается с месяца после подключения или с начала окна, если подключение
  было раньше.

Фактический объём (запуск 1):

| Таблица | Строк |
|---|---|
| dwh.dim_client | 1000 |
| dwh.dim_tariff | 4 |
| dwh.dim_date | 24 |
| dwh.fact_client_activity | **19 928** |

Ещё по фактическим данным `dwh`:

- строк факта на клиента: минимум 1, медиана 23, максимум 24, среднее 19,93; строки факта есть у всех 1000 клиентов;
- ИНН: 638 десятизначных, 362 двенадцатизначных; email, телефоны и ИНН уникальны (по 1000);
  уникальных ФИО 519, названий отелей 139, городов 26;
- `rooms_count` от 4 до 449; `connected_date` от 2024-01-01 до 2025-06-28.

**Доля churn_flag:**

| | Строк с churn_flag=1 | Доля строк | Ушедших клиентов | Доля клиентов |
|---|---|---|---|---|
| dwh | 269 из 19 928 | 1,35 % | 269 из 1000 | 26,9 % |
| ml | 223 из 16 045 | 1,39 % | 223 из 805 | 27,7 % |

## 5. DAG `mask_dwh_to_ml`

Параметры (`dags/mask_dwh_to_ml.py`): `schedule=None` (только ручной запуск), `catchup=False`,
`max_active_runs=1`, `is_paused_upon_creation=False`, `start_date = 2025-01-01 (Europe/Moscow)`,
теги `masking`, `clickhouse`, `demo`. Соль берётся из Variable `masking_salt` и проверяется
`check_salt` (длина не меньше 16). `K_MIN` берётся из Variable `k_min` (по умолчанию 5), её задаёт
переменная окружения `AIRFLOW_VAR_K_MIN` из `.env`.

| № | Задача | Что делает | Время, запуск 1 | Время, запуск 2 |
|---|---|---|---|---|
| 1 | `generate_dwh` | `synthetic.generate.generate()`: DROP + CREATE таблиц `dwh`, вставка синтетики, проверка сирот и зерна | 6,96 с | 1,10 с |
| 2 | `mask_dim_client` | проверка соли, сверка колонок `dwh` с `rules.yaml` (`check_rules_cover_schema`), `ddl_ml.sql`, `mask_dim_client.sql`; статистика и фактическое k | 0,41 с | 0,42 с |
| 3 | `copy_dims` | `copy_dims.sql`: TRUNCATE + копия `dim_tariff`, `dim_date` | 0,28 с | 0,28 с |
| 4 | `mask_fact` | `mask_fact.sql`: TRUNCATE + факт с псевдонимом, без строк подавленных клиентов | 0,33 с | 0,35 с |
| 5 | `validate_ml` | 8 проверок из `validate_ml.sql`; результат пишется в XCom `checks`; при любом FAIL задача падает | 0,48 с | 0,57 с |
| 6 | `write_log` | `trigger_rule=ALL_DONE`; собирает статистику из XCom трёх задач и `checks`, пишет строку в `ml.masking_log`; падает, если статус FAILED | 0,81 с | 0,83 с |

Время задачи считается от `start_date` до `end_date` экземпляра задачи. Весь запуск длился: 1 — 12,33 с,
2 — 7,44 с, 3 — 12,42 с, 4 — 13,16 с, 5 — 7,46 с. `generate_dwh` занимал 6,65–6,96 с в запусках 1, 3, 4
(первые после старта контейнера Airflow) и 1,10–1,11 с в запусках 2 и 5. Причина разницы не исследовалась.

**Повторная запускаемость** (по коду):

- `dwh` пересоздаётся (DROP + CREATE), генерация с фиксированным seed, поэтому данные каждый раз одинаковые;
- таблицы `ml` создаются через `CREATE TABLE IF NOT EXISTS`, представление через `CREATE OR REPLACE VIEW`;
- перед каждой вставкой в `ml.dim_client`, `ml.dim_tariff`, `ml.dim_date`, `ml.fact_client_activity`
  выполняется `TRUNCATE`, поэтому повтор не создаёт дублей;
- псевдоним детерминирован при той же соли;
- `ml.masking_log` не очищается и копит по строке на каждый запуск;
- `max_active_runs=1` не даёт двум запускам выполняться одновременно.

Подтверждено запусками: во всех пяти в `ml` 805 клиентов и 16 045 строк факта. MD5 отсортированного списка
псевдонимов (`hex(MD5(arrayStringConcat(arraySort(groupArray(client_key)), ',')))`) после запусков 1, 2, 3
и 4 одинаковый: `0B0A0228548F721F907AFEB8D5751DD7`. Запуск 3 шёл после пересоздания контейнера Airflow,
запуск 4 после старта с восстановленным `FERNET_KEY` (раздел 6.3). В `masking_log` после всех запусков пять строк.

## 6. Правила маскирования

### 6.1 По полям

Правила описаны в `masking/rules.yaml`, реализованы в SQL. Подстановки `@PSEUDO_KEY@` и `@QI_SOURCE@`
раскрывает `masker._render`.

| Поле `dwh` | Что с ним делается | Поле в `ml` | Метод | Реализация | Ключевой фрагмент |
|---|---|---|---|---|---|
| dim_client.client_key | заменяется псевдонимом | client_key | псевдонимизация: усечённый SHA-256 с секретной солью | `masker.py` (PSEUDO_KEY), `mask_dim_client.sql` | `lower(hex(substring(SHA256(concat({salt:String}, ':', toString(client_key))), 1, 8)))` |
| contact_name | не переносится | — | удаление | `mask_dim_client.sql` (нет в SELECT) | список колонок INSERT: `(client_key, email_masked, phone_masked, federal_district, rooms_range, connected_quarter)` |
| hotel_name | не переносится | — | удаление | то же | то же |
| inn | не переносится | — | удаление | то же | то же |
| email | маскируется | email_masked | маскирование: первый символ + `***` + домен | `mask_dim_client.sql` | `concat(leftUTF8(c.email, 1), '***', substring(c.email, position(c.email, '@')))` |
| phone | маскируется | phone_masked | маскирование: остаются последние 4 цифры | `mask_dim_client.sql` | `replaceRegexpAll(phone, '[^0-9]', '') AS digits` … `concat('+7 *** *** ', substring(digits, 8, 2), ' ', substring(digits, 10, 2))` |
| city | обобщается | federal_district | обобщение по справочнику (26 городов → 8 округов, неизвестный город → «Не определён») | `dwh_client_qi.sql`, справочник в `rules.yaml` | `transform(city, {cities:Array(String)}, {districts:Array(String)}, {unknown_district:String})` |
| rooms_count | обобщается | rooms_range | обобщение в диапазоны: до 20 / 21–50 / 51–100 / 100+ (границы 20, 50, 100) | `dwh_client_qi.sql` | `{room_labels:Array(String)}[arrayCount(e -> rooms_count > e, {room_edges:Array(UInt16)}) + 1]` |
| connected_date | обобщается | connected_quarter | обобщение до квартала `ГГГГ-QN` | `dwh_client_qi.sql` | `concat(toString(toYear(connected_date)), '-Q', toString(toQuarter(connected_date)))` |
| весь клиент из группы < K_MIN | не переносится | — | подавление записей (k-анонимность) | `mask_dim_client.sql`, `mask_fact.sql` | `WHERE q.group_size >= {k_min:UInt32}` |
| dim_tariff.* | копируется | dim_tariff.* | без изменений | `copy_dims.sql` | `INSERT INTO ml.dim_tariff SELECT tariff_key, tariff_name, price FROM dwh.dim_tariff` |
| dim_date.* | копируется | dim_date.* | без изменений | `copy_dims.sql` | `INSERT INTO ml.dim_date SELECT date_key, year, quarter, month FROM dwh.dim_date` |
| fact.client_key | заменяется тем же псевдонимом | client_key | псевдонимизация | `mask_fact.sql` | `@PSEUDO_KEY@ AS pseudo_key` |
| fact: показатели, tariff_key, date_key, churn_flag | копируются | те же | без изменений | `mask_fact.sql` | `tariff_key, date_key, logins_count, …, mrr, churn_flag` |
| fact: строки подавленных клиентов | не переносятся | — | подавление | `mask_fact.sql` | `WHERE client_key IN (SELECT src_key FROM (@QI_SOURCE@) WHERE group_size >= {k_min:UInt32})` |

Защита от пропуска колонки: `masker.check_rules_cover_schema` читает `system.columns` для каждой таблицы `dwh`
из `rules.yaml`. Если у колонки нет правила (и для таблицы не задано `"*"`), выбрасывается `RuntimeError`,
и `mask_dim_client` падает до маскирования. Негативным тестом эта ветка не проверялась.

Обобщённые значения (`federal_district`, `rooms_range`, `connected_quarter`) и маски вычисляются в SQL
при каждом запуске, а не хранятся отдельно.

### 6.2 Псевдоним

```
pseudo = lower(hex( first_8_bytes( SHA256( salt || ':' || toString(client_key) ) ) ))
```

- Результат: 16 hex-символов, то есть 64 бита. Коллизий в запуске 1 нет: 805 строк, 805 уникальных значений.
- Одинаковый `client_key` при той же соли всегда даёт тот же псевдоним, поэтому факт соединяется
  с измерением в `ml` без исходного ключа.
- Без соли перебор 1000 исходных ключей бесполезен. Если соль известна, перебор 1000 ключей обращает
  псевдоним сразу (раздел 13).
- Отдельной таблицы соответствия «ключ → псевдоним» нет. Роль ключа сопоставления выполняет соль вместе
  с формулой: зная их, соответствие вычисляется заново (раздел 12).
- Почему SHA-256: в README приведён замер на 10 млн строк (SHA-256 1,3 с, `sipHash64` 0,33 с,
  `cityHash64` 0,14 с). **В этом запуске он не повторялся**, это цифры из README.

### 6.3 Где хранится соль и как обеспечена её устойчивость

1. **Источник.** `MASKING_SALT` в `.env`. При старте контейнера Airflow выполняется `python -m masking.init_salt`
   (`docker-compose.yml`, `command`, под `set -e`):
   - `MASKING_SALT` задан → он записывается в Variable `masking_salt`;
   - Variable нет → генерируется `secrets.token_hex(32)` (64 hex-символа);
   - Variable есть и расшифровывается → используется она;
   - Variable есть, но не расшифровывается → процесс завершается с кодом 1, Airflow не стартует, новая соль
     не создаётся.

   В этом запуске `MASKING_SALT` был пустым, соль сгенерирована (`masking_salt: создана новая соль` в логе),
   длина 64.
2. **Хранение.** Airflow Variable `masking_salt` в базе метаданных Postgres (volume `pg_data`), в таблице
   `variable` `is_encrypted = t`. Ключ Fernet задаётся `AIRFLOW__CORE__FERNET_KEY` из `FERNET_KEY` в `.env`
   (`${FERNET_KEY:?нет .env}`, без него compose не стартует). До правок ключ генерировался внутри контейнера
   в `airflow.cfg` и терялся при пересоздании контейнера, из-за чего соль не расшифровывалась и заменялась новой.
3. **Проверка устойчивости.**
   - `docker compose up -d --force-recreate airflow` после запуска 2: в логе `masking_salt: используется
     сохранённая соль`, SHA-256 значения Variable до и после совпал, MD5 списка псевдонимов после запуска 3
     тот же `0B0A0228…`;
   - в `.env` подставлен другой `FERNET_KEY`, затем `--force-recreate airflow`: контейнер завершился
     (`Exited (1)`), в логе:
     ```
     [error] Can't decrypt _val for key=masking_salt, invalid token or value
     ОШИБКА: Variable masking_salt есть в базе Airflow, но не расшифровывается текущим FERNET_KEY.
     Новая соль не создаётся, иначе сменятся все псевдонимы. Верните в .env прежний FERNET_KEY
     или удалите данные (docker compose down -v).
     ```
   - прежний `FERNET_KEY` возвращён, `docker compose up -d airflow`: `используется сохранённая соль`,
     MD5 псевдонимов после запуска 4 тот же.
4. **git.** `.env` указан в `.gitignore`. Поиск значения соли по рабочей копии без `.git` нашёл 0 файлов.
5. **SQL-файлы.** Соль передаётся только как параметр `{salt:String}` через `parameters=` в clickhouse-connect.
   В текстах SQL её нет.
6. **system.query_log ClickHouse.** Для запросов с параметром `salt` `db.run_sql` выставляет
   `log_queries=0, log_query_threads=0, log_query_views=0`. После `SYSTEM FLUSH LOGS` в `query_log`
   3968 записей, значение соли не найдено ни в одной (поиск выполнялся по выгрузке текста запросов
   вне ClickHouse, чтобы сам поиск не попал в журнал). Запросов с `SHA256(concat` в журнале 0.
   От `etl_user` в журнале 468 записей `QueryFinish`, то есть остальные запросы логируются.
7. **Логи сервера ClickHouse.** Уровень `information` (`clickhouse/config/logging.xml`). Соль
   в `/var/log/clickhouse-server/` и `system.text_log`: 0 совпадений.
8. **Логи Airflow.** `AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES=salt`. Соль в `/opt/airflow/logs`: 0 файлов;
   в `docker compose logs` всех трёх сервисов: 0 совпадений.
9. **Таблицы ml.** В `ml.masking_log.checks_details` соли нет. Задача `validate_ml` соль не использует.
10. **Окружение контейнера.** `docker inspect`: `MASKING_SALT` пустой, `AIRFLOW__CORE__FERNET_KEY` задан
    и виден в окружении контейнера (раздел 13.2).

Не проверялось: показывает ли UI или REST API Airflow значение Variable `masking_salt`. CLI
`airflow variables get masking_salt` внутри контейнера возвращает его в открытом виде.

## 7. k-анонимность

### 7.1 Параметры

- **K_MIN = 5.** Источник: `.env` (`K_MIN=5`, как в `.env.example`) → `AIRFLOW_VAR_K_MIN` → Variable `k_min`.
  Значение по умолчанию в коде (`_k_min`) тоже 5.
- **Квазиидентификаторы:** `federal_district`, `rooms_range`, `connected_quarter`
  (`rules.yaml`, `k_anonymity.quasi_identifiers`, действие `on_violation: suppress`).
- **Расчёт по клиентам**, а не по строкам факта: в `dwh_client_qi.sql` считается
  `count() OVER (PARTITION BY federal_district, rooms_range, connected_quarter) AS group_size` по строкам
  `dwh.dim_client`. Фактическое k в `ml`: `min(count())` по группам `ml.dim_client`.
- **Подавленные клиенты** (`group_size < K_MIN`) целиком не попадают в `ml.dim_client`, и все их строки
  факта тоже не попадают в `ml.fact_client_activity`. Переобобщения и слияния групп в DAG нет.

### 7.2 Результат основного сценария (K_MIN = 5, квартал)

| Показатель | Значение |
|---|---|
| Групп по КИ в dwh (до подавления) | 155 (`qi_groups_total`) |
| Из них размером < 5 | 90 групп, в них 195 клиентов |
| Групп размером 1 в dwh | 31 |
| Размер групп в dwh: мин / медиана / макс | 1 / 4 / 39 |
| Подавлено клиентов | **195 из 1000 (19,5 %)** |
| Подавлено строк факта | **3 883 из 19 928 (19,5 %)** |
| Групп в ml | 65 |
| Размер групп в ml: мин / медиана / макс / среднее | **5 / 11 / 39 / 12,38** |
| Фактическое k | **5** |
| Клиентов с «Не определён» | 0 |

Размеры групп в `ml`: размер (число групп) 5 (9), 6 (7), 7 (8), 8 (3), 9 (2), 10 (1), 11 (5), 12 (6),
13 (1), 14 (6), 15 (1), 17 (1), 18 (1), 19 (3), 20 (1), 21 (2), 23 (1), 24 (4), 29 (1), 30 (1), 39 (1).

Распределение `connected_quarter` (dwh → ml): 2024-Q1 210 → 174, 2024-Q2 219 → 186, 2024-Q3 183 → 152,
2024-Q4 151 → 112, 2025-Q1 139 → 115, 2025-Q2 98 → 66. Распределения по округам и `rooms_range` приведены
в 7.3 (столбец «квартал, K_MIN=5»).

### 7.3 Эксперимент для оценки достаточности

Скрипт `masking/experiment.py` (SQL `masking/sql/experiment_k.sql`) считает сценарии по `dwh` той же
формулой КИ, что и DAG. Вариант «полугодие» заменяет `connected_quarter` на `ГГГГ-H1/H2`
(`intDiv(toQuarter(connected_date) + 1, 2)`). DAG и таблицы `ml` скрипт не меняет. Результат сохранён
в `experiments/k_min_experiment.md` и `experiments/k_min_experiment.csv`. Сценарий «квартал, K_MIN=5»
совпал с фактом DAG (805 клиентов, 16 045 строк, 65 групп), что подтверждает одинаковость формулы.

Основные показатели:

| Показатель | квартал, 3 | **квартал, 5** | квартал, 10 | полугодие, 3 | полугодие, 5 | полугодие, 10 |
|---|---|---|---|---|---|---|
| Групп по КИ в dwh | 155 | 155 | 155 | 85 | 85 | 85 |
| Групп в ml | 96 | 65 | 36 | 66 | 55 | 34 |
| Фактическое k | 3 | 5 | 10 | 3 | 5 | 10 |
| Клиентов в ml | 913 | 805 | 620 | 974 | 938 | 800 |
| Подавлено клиентов | 87 (8,7 %) | 195 (19,5 %) | 380 (38,0 %) | 26 (2,6 %) | 62 (6,2 %) | 200 (20,0 %) |
| Строк факта в ml | 18 285 | 16 045 | 12 452 | 19 448 | 18 774 | 15 876 |
| Подавлено строк факта | 1 643 (8,2 %) | 3 883 (19,5 %) | 7 476 (37,5 %) | 480 (2,4 %) | 1 154 (5,8 %) | 4 052 (20,3 %) |
| Доля ушедших клиентов: dwh → ml | 26,9 → 26,7 % | 26,9 → 27,7 % | 26,9 → 27,7 % | 26,9 → 26,8 % | 26,9 → 26,9 % | 26,9 → 27,4 % |
| Доля строк `churn_flag=1`: dwh → ml | 1,35 → 1,33 % | 1,35 → 1,39 % | 1,35 → 1,38 % | 1,35 → 1,34 % | 1,35 → 1,34 % | 1,35 → 1,38 % |

Клиентов по федеральным округам:

| Округ | dwh | квартал, 3 | квартал, 5 | квартал, 10 | полугодие, 3 | полугодие, 5 | полугодие, 10 |
|---|---|---|---|---|---|---|---|
| Центральный | 363 | 361 | 357 | 313 | 363 | 363 | 363 |
| Северо-Западный | 184 | 177 | 160 | 125 | 183 | 183 | 155 |
| Южный | 210 | 204 | 186 | 141 | 210 | 206 | 193 |
| Приволжский | 114 | 103 | 81 | 41 | 113 | 107 | 89 |
| Уральский | 43 | 29 | 5 | 0 | 36 | 33 | 0 |
| Сибирский | 34 | 14 | 10 | 0 | 25 | 19 | 0 |
| Дальневосточный | 31 | 17 | 6 | 0 | 26 | 15 | 0 |
| Северо-Кавказский | 21 | 8 | **0** | 0 | 18 | 12 | 0 |

Клиентов по `rooms_range`:

| rooms_range | dwh | квартал, 3 | квартал, 5 | квартал, 10 | полугодие, 3 | полугодие, 5 | полугодие, 10 |
|---|---|---|---|---|---|---|---|
| до 20 | 429 | 412 | 376 | 336 | 423 | 420 | 373 |
| 21–50 | 334 | 313 | 289 | 242 | 331 | 314 | 288 |
| 51–100 | 157 | 132 | 104 | 31 | 147 | 138 | 101 |
| 100+ | 80 | 56 | 36 | 11 | 73 | 66 | 38 |

Выводы по данным эксперимента:

- при квартале рост K_MIN с 5 до 10 удваивает потери (19,5 % → 38,0 % клиентов) и полностью убирает
  Уральский, Сибирский, Дальневосточный и Северо-Кавказский округа;
- обобщение до полугодия при K_MIN=5 сокращает подавление с 19,5 % до 6,2 % клиентов, все восемь округов
  остаются в данных, группа 100+ теряет 18 % клиентов вместо 55 %; ценой этого становится более грубая
  дата подключения;
- полугодие с K_MIN=10 по потерям (20,0 %) близко к основному сценарию, но четыре малых округа исчезают
  полностью;
- доля ушедших клиентов во всех сценариях остаётся в пределах 26,7–27,7 % при 26,9 % в `dwh`.
  Это проверка только целевой переменной, влияние на качество модели не оценивалось.

Основной сценарий DAG остаётся K_MIN=5, квартал. Переход на полугодие в DAG не выполнялся.

## 8. Разграничение доступа

Пользователей создаёт `clickhouse/initdb/01_users.sh` при первом старте ClickHouse (пустой volume
`ch_data`), аутентификация `sha256_password`. Пароли берутся из `.env`, в отчёт не включены.

| Пользователь | Назначение | Права (`SHOW GRANTS`, фактически) |
|---|---|---|
| `admin` | администратор (`CLICKHOUSE_USER`, `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`) | все, управление доступом |
| `etl_user` | Airflow: генерация, маскирование, проверки | `GRANT ALL ON dwh.*`, `GRANT ALL ON ml.*` |
| `ml_user` | потребитель обезличенных данных (модель, аналитик) | `GRANT SELECT ON ml.masking_log`, `GRANT SELECT ON ml.v_ml_client_month` |

**SQL SECURITY DEFINER.** В ClickHouse 26.8 обычное представление по умолчанию выполняется с правами
вызывающего (`default_normal_view_sql_security = INVOKER`). Проверено: представление без `SQL SECURITY`,
на которое выдан `SELECT` пользователю `ml_user`, возвращает
`Code: 497 … necessary to have the grant SELECT ON ml.dim_client. (ACCESS_DENIED)`. Поэтому
`ml.v_ml_client_month` создаётся с `DEFINER = CURRENT_USER SQL SECURITY DEFINER` и выполняется с правами
`etl_user`.

Фактический вывод под `ml_user` (`clickhouse-client`, после запуска 1):

| Запрос | Результат |
|---|---|
| `SHOW DATABASES` | `ml` |
| `SHOW TABLES FROM ml` | `masking_log`, `v_ml_client_month` |
| `SELECT count(), uniqExact(client_key), sum(churn_flag) FROM ml.v_ml_client_month` | 16045, 805, 223 |
| `SELECT count() FROM ml.masking_log` | 1 |
| `SELECT * FROM ml.dim_client LIMIT 1` | `Code: 497 … necessary to have the grant SELECT ON ml.dim_client. (ACCESS_DENIED)` |
| `SELECT * FROM ml.fact_client_activity LIMIT 1` | `Code: 497 … SELECT ON ml.fact_client_activity. (ACCESS_DENIED)` |
| `SELECT * FROM ml.dim_tariff LIMIT 1`, `ml.dim_date` | `Code: 497 … (ACCESS_DENIED)` |
| `SELECT * FROM dwh.dim_client LIMIT 1` | `Code: 497 … SELECT ON dwh.dim_client. (ACCESS_DENIED)` |
| `SELECT email_masked FROM ml.v_ml_client_month` | `Code: 47 … Unknown expression identifier email_masked` (колонки в представлении нет) |
| `INSERT INTO ml.masking_log (run_id) VALUES ('x')` | `Code: 497 … INSERT(run_id) ON ml.masking_log. (ACCESS_DENIED)` |

Запросы `checks.sql` к `ml.dim_client` (3.1, 3.2) и `ml.fact_client_activity` (1.3) выполняются под `etl_user`.
Проверки отказа для `ml_user` вынесены в раздел 4 `checks.sql` (4.1–4.4).

## 9. Проверки `validate_ml`

Источник: `masking/sql/validate_ml.sql`. Подавленные клиенты для балансов пересчитываются по `dwh` той же
формулой (`@QI_SOURCE@`), а не берутся из результата маскирования. Задача падает, если хотя бы у одной
проверки `ok = 0`.

| Проверка | Что проверяет | Условие падения | Факт (запуск 1) |
|---|---|---|---|
| `clients_balance` | клиентов в dwh = клиентов в ml + подавлено | `dwh_n ≠ ml_n + suppressed` | OK: dwh=1000, ml=805, подавлено=195 |
| `fact_balance` | строк факта в dwh = строк в ml + строки подавленных | `dwh_n ≠ ml_n + suppressed` | OK: dwh=19928, ml=16045, подавлено=3883 |
| `activity_sums` | суммы 8 показателей в ml = суммы по неподавленным клиентам dwh | кортежи сумм не равны | OK: (930147, 58439, 969199, 191458, 30152, 1655, 210820500, 223) в обоих слоях |
| `fact_orphans` | у каждой строки факта ml есть пара в `dim_client`, `dim_tariff`, `dim_date` | сумма сирот > 0 | OK: без клиента=0, без тарифа=0, без даты=0 |
| `pseudonyms_unique` | нет коллизий псевдонимов | `count() ≠ uniqExact(client_key)` | OK: строк=805, уникальных=805 |
| `no_source_keys` | исходных `client_key` (как строк) нет в `ml.dim_client` и `ml.fact_client_activity` | найдено > 0 | OK: dim_client=0, fact=0 |
| `no_pii` | в `ml.dim_client` нет исходных email, телефонов (исходный формат и только цифры), ИНН, ФИО, названий отелей; маски соответствуют шаблонам `^[^@][*][*][*]@[^@]+$` и `^[+]7 [*][*][*] [*][*][*] [0-9]{2} [0-9]{2}$`; в `ml` нет колонок `contact_name, email, phone, inn, hotel_name, city, rooms_count, connected_date` | строк с нарушением + PII-колонок > 0 | OK: строк с PII или неверной маской=0, PII-колонок=0 |
| `k_anonymity` | фактическое k в `ml.dim_client` ≥ K_MIN | `k_actual < K_MIN` | OK: k=5, K_MIN=5 |

В запусках 1–5 все восемь проверок OK, статус PASSED.

**Негативный тест (выполнен).** После запуска 4 в `ml.dim_client` вставлена строка клиента 1 с исходным
ключом `'1'` и исходным email, затем `masker.validate(5)` вызван вручную внутри контейнера Airflow:

```
FAIL clients_balance — dwh=1000, ml=806, подавлено=195
OK   fact_balance — dwh=19928, ml=16045, подавлено=3883
OK   activity_sums — … (суммы совпали)
OK   fact_orphans — без клиента=0, без тарифа=0, без даты=0
OK   pseudonyms_unique — строк=806, уникальных псевдонимов=806
FAIL no_source_keys — исходных ключей: dim_client=1, fact=0
FAIL no_pii — строк с PII или неверной маской=1, PII-колонок в ml=0
OK   k_anonymity — k=5, K_MIN=5
```

Результат совпадает с README («падает на `clients_balance`, `no_source_keys`, `no_pii`»). Тест запускался
не через DAG, поэтому в `masking_log` строки FAILED нет; ветка `write_log` со статусом FAILED через DAG
не проверялась. Слой `ml` восстановлен запуском 5.

Чего проверки не покрывают: в `no_pii` проверяется только `ml.dim_client`, факт на PII не проверяется
(PII-колонок в нём нет по DDL). Уникальность масок и рядов активности не проверяется (раздел 13).
Права `ml_user` в `validate_ml` не проверяются, они проверены вручную (раздел 8).

## 10. Результаты запуска в цифрах

**run_id:** `manual__2026-11-09T12:16:36.326553+00:00`. Состояние `success`,
старт 12:16:36.633, конец 12:16:48.965 UTC (12,33 с). Время задач приведено в разделе 5.

| Таблица | Прочитано (dwh) | Записано (ml) | Не перенесено |
|---|---|---|---|
| dim_client | 1000 | 805 | 195 (подавлены) |
| dim_tariff | 4 | 4 | 0 |
| dim_date | 24 | 24 | 0 |
| fact_client_activity | 19 928 | 16 045 | 3 883 (строки подавленных) |

- Подавлено клиентов: **195 (19,5 %)**, строк факта: **3 883 (19,5 %)**.
- Фактическое k: **5** при K_MIN = 5.
- Группы в ml: **65**, размер мин / медиана / макс: **5 / 11 / 39**.
- Витрина `ml.v_ml_client_month` под `ml_user`: 16 045 строк, 805 псевдонимов, `sum(churn_flag)` = 223.

Строки `ml.masking_log` после всех запусков:

| run_id | checks_status | written dim_client / fact | suppressed clients / fact | k_min / k_actual |
|---|---|---|---|---|
| manual__2026-11-09T12:16:36.326553+00:00 | PASSED | 805 / 16045 | 195 / 3883 | 5 / 5 |
| manual__2026-11-09T12:17:28.663423+00:00 | PASSED | 805 / 16045 | 195 / 3883 | 5 / 5 |
| manual__2026-11-09T12:18:22.043090+00:00 | PASSED | 805 / 16045 | 195 / 3883 | 5 / 5 |
| manual__2026-11-09T12:20:17.955136+00:00 | PASSED | 805 / 16045 | 195 / 3883 | 5 / 5 |
| manual__2026-11-09T12:20:44.612581+00:00 | PASSED | 805 / 16045 | 195 / 3883 | 5 / 5 |

`checks_details` содержит JSON-массив из 8 объектов `{check, description, ok, details}`; значения `details`
совпадают с колонкой «Факт» в разделе 9.

Цифры «Сценария показа» README (805 клиентов, 16 045 строк, 195 подавленных, 3 883 строки, k = 5) совпали
с фактом.

## 11. Примеры (синтетические данные)

### 11.1 До и после маскирования: dim_client

Записи `dwh.dim_client` (клиенты 1–5 и 24) и соответствующие строки `ml.dim_client`. Сопоставление по
вычисленному псевдониму выполнено под `etl_user` внутри контейнера Airflow с `log_queries=0`. Псевдонимы
относятся к соли этого стенда (запуски 1–5). Клиент 24 используется в `checks.sql`.

| client_key | contact_name | email | phone | inn | hotel_name | city | rooms | connected |
|---|---|---|---|---|---|---|---|---|
| 1 | Егоров Владимир Александрович | vladimir.egorov839@list.ru | +7 (938) 623-46-42 | 2588585010 | Гостиница «Волга» | Москва | 287 | 2024-08-22 |
| 2 | Павлов Владимир Александрович | vladimir.pavlov43@list.ru | +7 (997) 913-44-72 | 171892052237 | Бутик-отель «Старый город» | Москва | 46 | 2025-05-25 |
| 3 | Михайлова Юлия Александровна | yuliya.mikhaylova820@mail.ru | +7 (933) 859-75-40 | 849773903540 | Апарт-отель «Лазурь» | Москва | 22 | 2024-10-12 |
| 4 | Козлова Ксения Петровна | kseniya.kozlova530@mail.ru | +7 (911) 868-76-18 | 460056023584 | Гостевой дом «Тайга» | Москва | 39 | 2025-03-12 |
| 5 | Орлов Павел Дмитриевич | pavel.orlov870@yandex.ru | +7 (978) 756-37-83 | 8395979147 | Апарт-отель «Северная звезда» | Москва | 62 | 2024-04-14 |
| 24 | Фёдорова Ольга Андреевна | olga.fedorova209@gmail.com | +7 (937) 740-13-51 | 841534193728 | Отель «Купеческий» | Краснодар | 49 | 2024-01-15 |

↓

| (исх. ключ) | client_key (псевдоним) | email_masked | phone_masked | federal_district | rooms_range | connected_quarter |
|---|---|---|---|---|---|---|
| 1 | 127a923533b9cb46 | v***@list.ru | +7 *** *** 46 42 | Центральный | 100+ | 2024-Q3 |
| 2 | 166600db574b0325 | v***@list.ru | +7 *** *** 44 72 | Центральный | 21–50 | 2025-Q2 |
| 3 | 942061729c49beb9 | y***@mail.ru | +7 *** *** 75 40 | Центральный | 21–50 | 2024-Q4 |
| 4 | c8ee2ca2bb20d18b | k***@mail.ru | +7 *** *** 76 18 | Центральный | 21–50 | 2025-Q1 |
| 5 | 62882edfc952a106 | p***@yandex.ru | +7 *** *** 37 83 | Центральный | 51–100 | 2024-Q2 |
| 24 | 8244e67e2c95c700 | o***@gmail.com | +7 *** *** 13 51 | Южный | 21–50 | 2024-Q1 |

Колонка «исх. ключ» добавлена только для отчёта, в `ml` её нет. Примеры подавленных клиентов: 12
(Санкт-Петербург, 270 номеров, 2024-05-02, 24 строки факта), 40 (Хабаровск, 20, 2025-02-10, 20 строк),
44 (Новосибирск, 76, 2025-02-28, 12 строк). Их псевдонимов нет ни в `ml.dim_client`, ни в `ml.fact_client_activity`.

### 11.2 Итоговая витрина для ML

`SELECT * FROM ml.v_ml_client_month ORDER BY client_key, date_key LIMIT 5` под `ml_user`:

| client_key | date_key | year | quarter | month | federal_district | rooms_range | connected_quarter | tariff_key | tariff_name | tariff_price | logins_count | active_users | bookings_processed | features_used | tickets_count | late_payments | mrr | churn_flag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 005245c8dcb7931f | 2024-11-01 | 2024 | 4 | 11 | Северо-Западный | до 20 | 2024-Q1 | 2 | Бизнес | 9900 | 17 | 1 | 8 | 14 | 4 | 0 | 9900 | 0 |
| 005245c8dcb7931f | 2024-12-01 | 2024 | 4 | 12 | Северо-Западный | до 20 | 2024-Q1 | 2 | Бизнес | 9900 | 17 | 1 | 12 | 14 | 3 | 0 | 9900 | 0 |
| 005245c8dcb7931f | 2025-01-01 | 2025 | 1 | 1 | Северо-Западный | до 20 | 2024-Q1 | 2 | Бизнес | 9900 | 22 | 1 | 9 | 13 | 3 | 1 | 9900 | 0 |
| 005245c8dcb7931f | 2025-02-01 | 2025 | 1 | 2 | Северо-Западный | до 20 | 2024-Q1 | 2 | Бизнес | 9900 | 14 | 1 | 16 | 13 | 2 | 0 | 9900 | 0 |
| 005245c8dcb7931f | 2025-03-01 | 2025 | 1 | 3 | Северо-Западный | до 20 | 2024-Q1 | 2 | Бизнес | 9900 | 17 | 1 | 9 | 12 | 2 | 0 | 9900 | 0 |

Отток по округам из витрины под `ml_user` (запрос 2.2 из `checks.sql`): Центральный 357 клиентов / 91 ушёл
(0,255), Южный 186 / 56 (0,301), Северо-Западный 160 / 44 (0,275), Приволжский 81 / 29 (0,358),
Сибирский 10 / 0 (0), Дальневосточный 6 / 1 (0,167), Уральский 5 / 2 (0,4).

## 12. Сопоставление с приказом Роскомнадзора № 140

**Источник:** приказ Роскомнадзора от 19.06.2025 № 140 «Об утверждении требований к обезличиванию
персональных данных и методов обезличивания персональных данных, за исключением случаев, указанных
в пункте 9.1 части 1 статьи 6 Федерального закона от 27 июля 2006 г. № 152-ФЗ "О персональных данных"»,
зарегистрирован в Минюсте 31.07.2025 № 83110, вступил в силу 01.09.2025. Приказом признан утратившим
силу приказ Роскомнадзора от 05.09.2013 № 996. Приложение № 1 содержит требования к обезличиванию,
приложение № 2 — методы обезличивания.

Данные в проекте синтетические, оператора и локальных актов нет. Поэтому ниже речь идёт о том, что
проект спроектирован с учётом требований приказа, а не о соответствии приказу.

### 12.1 Методы (приложение № 2)

Пункт 1 приложения № 2 допускает применение методов из пунктов 2, 4, 10, 11 и 12 по отдельности или
в совокупности. В проекте применяется сочетание метода введения идентификаторов (п. 2) и метода изменения
состава или семантики (п. 4) в форме удаления (п. 6, 7) и изменения атрибутов (п. 9).

| Что в проекте | Пункт приказа | Комментарий |
|---|---|---|
| `client_key` → псевдоним SHA-256 с солью | метод введения идентификаторов, п. 2; ключ, п. 3 | идентификатор «в виде кода… имеющего постоянное… значение, присваиваемый на основании заданных оператором алгоритмов». Ключом сопоставления служит соль вместе с формулой (12.3) |
| удаление `contact_name`, `hotel_name`, `inn` | удаление атрибутов, пп. 5(1), 6, 7 | атрибуты, обработка которых не нужна для целей (модель оттока), исключаются из массива для всех субъектов (п. 6, 7) |
| маски `email` и `phone` с заменой части значения на `*` | изменение атрибутов, п. 9(2) | «замены части значений атрибута персональных данных на знак "*"» |
| `city` → округ, `rooms_count` → диапазон, `connected_date` → квартал | изменение атрибутов, п. 9(1) | «замены значений… обобщенными значениями… укрупнении значения атрибута персональных данных в пределах группы атрибутов» |
| подавление групп меньше K_MIN | ближайшее: удаление атрибутов отдельных субъектов, п. 7 | п. 7 допускает удаление атрибутов, относящихся к отдельным субъектам. **k-анонимность в приказе прямо не названа**; порог K_MIN и критерий размера группы — решение проекта, а не требование приказа |
| перемешивание (п. 10), декомпозиция (п. 11), преобразование массива (п. 12) | не применяются | k-анонимность (обобщение КИ до групп не меньше K_MIN) по смыслу близка к подпункту 1 п. 12 «обобщения (агрегации) атрибутов персональных данных субъектов». Это интерпретация, а не формулировка приказа |
| искажение атрибутов, п. 5(2), п. 8; замена на случайные значения, п. 9(3) | не применяются | показатели факта переносятся без искажения |

### 12.2 Требования (приложение № 1), фактическое состояние после правок

| Пункт | Требование (кратко) | Что есть в проекте | Оценка |
|---|---|---|---|
| п. 1(1) | использование методов из приложения № 2 | методы из 12.1: пп. 2, 3, 5(1), 6, 7, 9(1), 9(2) | реализовано технически |
| п. 1(2) | определение до обезличивания состава ПДн и субъектов | `rules.yaml`: правило на каждое поле каждой таблицы `dwh`; `check_rules_cover_schema` останавливает DAG до маскирования, если у колонки нет правила. Субъекты — все клиенты `dwh.dim_client` | реализовано технически; ветка с колонкой без правила негативным тестом не проверялась; формального перечня оператора нет |
| п. 1(3) | оценка достаточности метода для целей обработки | `validate_ml` (8 проверок, включая фактическое k) и эксперимент 7.3: потери клиентов и строк факта, сдвиг распределений по округам и `rooms_range`, доля оттока для K_MIN 3/5/10 и квартала/полугодия | частично: оценка выполнена по КИ и целевой переменной; ряды активности в факте остаются уникальными (13.1 п. 2), их достаточность не оценивалась; влияние на качество модели не оценивалось |
| п. 1(4) | невозможность определить субъекта без дополнительной информации | псевдоним без соли не обращается; прямые идентификаторы удалены; КИ обобщены, группы ≥ 5 | частично: уникальные ряды активности и почти уникальные маски телефона в `ml.dim_client` позволяют связать запись с внешним источником, где есть те же данные (13.1) |
| п. 1(5) | безопасность и конфиденциальность ИС обезличивания | RBAC ClickHouse: `ml_user` видит только витрину и журнал; соль не попадает в `query_log`, логи, git (6.3) | частично: нет TLS (HTTP 8123), Airflow без аутентификации, порты 8080 и 8123 опубликованы на хосте |
| п. 1(6) | исключение совместного хранения исходного и обезличенного массивов | `dwh` и `ml` — разные базы с разными правами | **не выполнено**: `dwh` и `ml` хранятся в одном экземпляре ClickHouse, `etl_user` имеет доступ к обоим. Это отличие прототипа от промышленного внедрения, где обезличенный массив выносится в отдельный экземпляр или хранилище; архитектура прототипа не менялась |
| п. 1(7) | учёт действий по обезличиванию и операций с обезличенными данными | `ml.masking_log`: строка на каждый запуск DAG со счётчиками и результатами проверок | частично: учёт обезличивания есть для запусков DAG; операции с обезличенными данными (чтение `ml_user`) фиксируются только в `system.query_log` ClickHouse, отдельного журнала нет; ручные действия (как негативный тест в разделе 9) не журналируются |
| п. 2 | локальные акты оператора (порядок обезличивания, правила изменения атрибутов, хранение актов отдельно) | — | не разрабатывались, вне рамок прототипа. `rules.yaml` описывает правила технически, но локальным актом не является |
| прил. № 2, п. 3 | ключ сопоставления хранится отдельно от массива и не передаётся третьим лицам | соль хранится вне ClickHouse: Airflow Variable в Postgres, зашифрована Fernet-ключом из `.env` | частично (12.3) |

### 12.3 Ключ сопоставления (приложение № 2, п. 3)

Пункт 3 требует при введении идентификаторов создать ключ, позволяющий сопоставить идентификаторы
и персональные данные, хранить его отдельно от массива и не передавать третьим лицам.

В проекте отдельной таблицы соответствия «`client_key` → псевдоним» нет. Роль ключа выполняет соль
вместе с формулой `SHA256(соль ‖ ':' ‖ client_key)`: при наличии соли и доступа к `dwh` соответствие
вычисляется заново (так получены примеры в 11.1). Соль хранится вне ClickHouse: в базе метаданных Airflow
(Postgres), зашифрована Fernet-ключом, который задаётся в `.env`. В `ml`, SQL-файлах, git и журналах её нет
(6.3). Ограничения: демонстрационный `FERNET_KEY` лежит в `.env.example` в открытом репозитории,
Airflow открыт без пароля, а `airflow variables get` в контейнере выдаёт соль открытым текстом (13.2).

## 13. Ограничения и слабые места

Ниже факты, проверенные запросами на данных запуска 1 (805 клиентов в `ml`), если не сказано иное.

### 13.1 Риски реидентификации

1. **Маски email и телефона почти уникальны.** В `ml.dim_client`:
   - `phone_masked`: 771 разное значение. У **738 из 805** клиентов (91,7 %) маска телефона уникальна:
     последние 4 цифры почти однозначно указывают на клиента;
   - `email_masked`: 78 разных значений, уникально только у 1 клиента;
   - пара (`email_masked`, `phone_masked`) уникальна у **всех 805**;
   - (квазиидентификаторы + `email_masked`) уникальны у 627 клиентов.

   k-анонимность считается только по трём квазиидентификаторам, маски в расчёт не входят. После правок
   маски недоступны `ml_user` (в `v_ml_client_month` их нет, `ml.dim_client` закрыта), но хранятся в `ml`
   и доступны `etl_user` и администратору. Тот, у кого есть доступ к `ml.dim_client` и телефоны клиентов
   из внешнего источника, восстанавливает связь «псевдоним → клиент» для большинства записей.
2. **Ряды активности уникальны.** В `ml.fact_client_activity`:
   - помесячный ряд `logins_count` уникален у **805 из 805** клиентов;
   - ряд `mrr` и ряд тарифов уникальны у 195 клиентов (24 %);
   - тройка (первый месяц, последний месяц, был ли уход) уникальна у 49 клиентов;
   - в одном месяце (2026-01) пара (`logins_count`, `bookings_processed`) уникальна у 563 строк из 693.

   Те же ряды доступны `ml_user` через `v_ml_client_month`. Уникальный ряд сам по себе не раскрывает
   личность, но позволяет связать записи с внешним источником, где есть тот же ряд или его часть (выгрузка
   биллинга, другая копия данных). Проект использует это в `checks.sql` 1.3: клиент находится в `ml`
   по совпадению ряда. k-анонимность защищает только измерение и на факт не распространяется.
3. **Стабильный псевдоним.** При той же соли псевдоним одинаков во всех запусках, теперь и после
   пересоздания контейнера Airflow. Выгрузки разных периодов связываются между собой и с уже
   реидентифицированными записями.
4. **Кто знает соль, обращает псевдонимы.** Ключи — это 1…1000, перебор занимает миллисекунды. Соль
   доступна всем, у кого есть доступ к Airflow: `airflow variables get masking_salt` в контейнере
   возвращает её открытым текстом (проверено), вход в UI Airflow без пароля. Доступ к значению через UI
   и REST API не проверялся.
5. **Усечение до 64 бит.** Коллизий на 805 значениях нет (проверено), на больших объёмах не оценивалось.
6. **Нет l-разнообразия и t-близости.** В группах может совпадать `churn_flag` или тариф. Не проверялось.
7. **Смещение выборки.** При K_MIN=5 и квартале подавлено 19,5 % клиентов, неравномерно: Северо-Кавказский
   округ пропал полностью, крупные отели (100+) потеряли 55 %. Эксперимент 7.3 показывает, что обобщение
   до полугодия снижает потери до 6,2 %. Влияние на качество модели оттока не оценивалось.

### 13.2 Слабые места реализации

1. **Демонстрационный `FERNET_KEY` в `.env.example`.** Ключ лежит в репозитории. Любой, у кого есть копия
   репозитория и доступ к базе Postgres Airflow, расшифрует соль. Для любого использования кроме демо ключ
   нужно генерировать заново (README, раздел «Запуск»). Ключ виден также в окружении контейнера
   (`docker inspect`).
2. **Смена `FERNET_KEY` останавливает Airflow.** Это намеренное поведение (6.3), но ротации Fernet-ключа
   с перешифровкой Variable (`airflow rotate-fernet-key`) в проекте нет.
3. Если `MASKING_SALT` задан в `.env`, он виден в окружении контейнера (`docker inspect`, `/proc/1/environ`):
   `unset` действует только на shell-процесс. Следует из конфигурации; в запуске соль не задавалась
   (`MASKING_SALT` пустой в `docker inspect`). Явно заданный `MASKING_SALT` перезаписывает сохранённую соль
   при каждом старте.
4. `log_queries=0` выставляется только в `db.run_sql`. Ручной запрос с солью в тексте (например,
   вычисление псевдонима администратором) попадёт в `system.query_log`.
5. `etl_user` имеет `ALL` на обе базы: одна учётная запись с доступом и к ПДн, и к обезличенным данным;
   её пароль лежит в окружении контейнера Airflow.
6. `dwh` и `ml` находятся в одном экземпляре ClickHouse: разделение логическое (RBAC), физически хранилище
   одно (п. 1(6) приложения № 1 не выполнен, раздел 12.2).
7. Нет TLS (HTTP 8123), порты 8123 и 8080 опубликованы на хосте.
8. Права `ml_user` выдаются в `initdb` только при первом старте ClickHouse. На существующем volume `ch_data`
   правка `01_users.sh` не применится без `docker compose down -v` или ручного `REVOKE`/`GRANT`.
9. `ml_user` видит `ml.masking_log` целиком, включая `checks_details` с суммами показателей.

### 13.3 Что не проверялось

- работа на объёмах больше 1000 клиентов, производительность (замер SHA-256 из README не повторялся);
- ветка `write_log` со статусом FAILED через DAG; падение `check_rules_cover_schema` на колонке без правила;
  поведение при неизвестном городе (в синтетике таких 0);
- запуск DAG с K_MIN, отличным от 5, и с обобщением до полугодия: значения 3 и 10 и вариант с полугодием
  посчитаны только скриптом эксперимента по `dwh`, через DAG не прогонялись;
- атаки с внешними данными (связывание по телефону или ряду MRR выполнено только как подсчёт уникальности,
  атака не моделировалась);
- доступ к соли через UI и REST API Airflow;
- ротация `FERNET_KEY` (`airflow rotate-fernet-key`);
- минимальные требования к памяти;
- обучение модели оттока на `ml` и сравнение с моделью на `dwh`.

### 13.4 Отличия от промышленного внедрения

- данные синтетические и детерминированные, `dwh` пересоздаётся генератором; в реальности `dwh` — источник,
  его нельзя DROP;
- полная перезаливка вместо инкремента;
- исходный и обезличенный массивы в одном экземпляре ClickHouse (п. 1(6) приложения № 1);
- соль в Airflow Variable без внешнего хранилища секретов и без ротации, Fernet-ключ в `.env`;
- Airflow `standalone` без аутентификации — режим разработки (в логе контейнера: «This is a development/test
  feature only. NEVER use it in production!»);
- нет локальных актов (п. 2 приложения № 1), назначенных лиц, журнала ручных действий и операций с обезличенными данными.

## 14. Как показать проект преподавателю

### 14.1 Запуск

Нужен Docker с Compose v2. В корне проекта:

```bash
cp .env.example .env
```

```bash
docker compose up -d
```

Через 1–2 минуты (Airflow при старте ставит clickhouse-connect):

- Airflow: http://localhost:8080, вход без пароля;
- ClickHouse Play: http://localhost:8123/play, пользователи `etl_user` и `ml_user`, пароли из `.env`.

Запуск DAG в UI (`mask_dwh_to_ml` → **Trigger**) или из консоли:

```bash
docker compose exec airflow airflow dags trigger mask_dwh_to_ml
```

Эксперимент K_MIN (после хотя бы одного запуска DAG):

```bash
docker compose exec -T airflow python -m masking.experiment md
```

Остановить с удалением данных (соль и псевдонимы будут новыми):

```bash
docker compose down -v
```

Если на машине уже есть volumes от предыдущей версии проекта, перед показом нужен `docker compose down -v`:
права `ml_user` и Fernet-ключ применяются только к новым данным. Пересоздание контейнера Airflow
(`--force-recreate`) псевдонимы не меняет, пока `FERNET_KEY` в `.env` прежний.

### 14.2 Порядок демонстрации

1. Airflow → DAG `mask_dwh_to_ml` → **Trigger**. Во вкладке **Graph** шесть задач, все зелёные
   за ≈ 7–13 с (в запусках 1–5: от 7,44 до 13,16 с).
2. Лог задачи `validate_ml`: восемь строк `[OK  ]` с цифрами из раздела 9.
3. ClickHouse Play под `etl_user`: запросы 1.1–1.3 из `checks.sql`. Клиент 24 в `dwh` с ПДн, его
   помесячная активность, тот же клиент в `ml` (псевдоним, `o***@gmail.com`, `+7 *** *** 13 51`,
   Южный / 21–50 / 2024-Q1). Найти его удаётся только по совпадению ряда активности, это иллюстрирует
   риск из 13.1 п. 2. Затем 3.1–3.2: фактическое k и малые группы.
4. Под `ml_user`: 2.1–2.2 (витрина для ML и отток по округам), 3.3 (журнал), 4.1 (`SHOW GRANTS`),
   4.2–4.4 (`ACCESS_DENIED` на `dwh.dim_client`, `ml.dim_client`, `ml.fact_client_activity`).
5. Повторный **Trigger**: в `ml` те же 805 / 16 045, псевдонимы те же, в `masking_log` новая строка.
6. Таблицы эксперимента: `experiments/k_min_experiment.md` или раздел 7.3.

### 14.3 Запросы для ClickHouse Play

Play выполняет всё поле как один запрос, поэтому запросы вставляются по одному. Полный набор лежит
в `checks.sql`, ниже основные.

Под `etl_user`:

```sql
SELECT client_key, contact_name, email, phone, inn, hotel_name, city, rooms_count, connected_date
FROM dwh.dim_client WHERE client_key = 24;
```

```sql
WITH (SELECT arraySort(groupArray((date_key, logins_count, bookings_processed, mrr)))
      FROM dwh.fact_client_activity WHERE client_key = 24) AS dwh_series
SELECT c.client_key AS pseudonym, c.email_masked, c.phone_masked,
       c.federal_district, c.rooms_range, c.connected_quarter,
       f.date_key, f.logins_count, f.bookings_processed, f.mrr, f.churn_flag
FROM ml.fact_client_activity AS f
INNER JOIN ml.dim_client AS c ON f.client_key = c.client_key
WHERE f.client_key IN (SELECT client_key FROM ml.fact_client_activity GROUP BY client_key
                       HAVING arraySort(groupArray((date_key, logins_count, bookings_processed, mrr))) = dwh_series)
ORDER BY f.date_key;
```

```sql
SELECT min(clients) AS k_actual, count() AS groups, sum(clients) AS clients_total
FROM (SELECT federal_district, rooms_range, connected_quarter, count() AS clients
      FROM ml.dim_client GROUP BY federal_district, rooms_range, connected_quarter);
```

Под `ml_user`:

```sql
SELECT * FROM ml.v_ml_client_month ORDER BY client_key, date_key LIMIT 20;
```

```sql
SELECT run_id, logged_at, dim_client_read, dim_client_written, fact_read, fact_written,
       suppressed_clients, suppressed_fact_rows, k_min, k_actual, checks_status
FROM ml.masking_log ORDER BY logged_at DESC;
```

```sql
SELECT * FROM ml.dim_client LIMIT 5;   -- ожидается Code: 497 ACCESS_DENIED
```

Запросы для раздела о рисках, под `etl_user` (их нет в `checks.sql`):

```sql
SELECT uniqExact(phone_masked) AS distinct_phone_masks,
       (SELECT countIf(n = 1) FROM (SELECT count() AS n FROM ml.dim_client GROUP BY phone_masked)) AS clients_with_unique_phone_mask,
       count() AS clients
FROM ml.dim_client;
```

```sql
SELECT countIf(n = 1) AS clients_with_unique_logins_series, count() AS distinct_series
FROM (SELECT s, count() AS n
      FROM (SELECT client_key, groupArraySorted(100)((date_key, logins_count)) AS s
            FROM ml.fact_client_activity GROUP BY client_key)
      GROUP BY s);
```
