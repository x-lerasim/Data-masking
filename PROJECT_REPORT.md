# Отчёт по проекту: обезличивание персональных данных между двумя слоями ClickHouse

Предмет: «ИИ для информационной безопасности». Репозиторий `x-lerasim/data-masking`, коммит `ab9a503`
(код при подготовке отчёта не менялся). Дата запуска: 2026-11-09, время в UTC.

## 0. Как получены цифры

Все цифры ниже взяты из реального запуска 2026-11-09 на стенде из `docker-compose.yml`. Если число взято
из кода или README, а не из запуска, это сказано рядом с ним.

Условия запуска (для воспроизводимости):

- Linux, 4 CPU, 15,7 GiB RAM, Docker 29.9.2, Docker Compose v5.6.1.
- `.env` создан копированием `.env.example`. `MASKING_SALT` пустой, поэтому соль сгенерирована при старте.
- Особенность стенда, к проекту не относится: исходящий HTTPS идёт через прокси с собственным CA, и
  `pip` внутри контейнера Airflow сначала не смог поставить `clickhouse-connect` (`CERTIFICATE_VERIFY_FAILED`).
  Чтобы это обойти, добавлен внешний override-файл, который монтирует CA-сертификат и задаёт `PIP_CERT`.
  Он лежит вне репозитория, код проекта и `docker-compose.yml` не менялись. На обычной машине этот шаг не нужен.

Выполненные запуски DAG `mask_dwh_to_ml`:

| № | run_id | Что проверялось | Итог |
|---|---|---|---|
| 1 | `manual__2026-11-09T11:22:22.783054+00:00` | основной запуск | success, PASSED |
| 2 | `manual__2026-11-09T11:23:44.109631+00:00` | повторная запускаемость | success, PASSED |
| – | без run_id, `masker.validate(5)` вручную | негативный тест (раздел 9) | 3 проверки FAIL |
| 3 | `manual__2026-11-09T11:25:28.809184+00:00` | восстановление `ml` после негативного теста | success, PASSED |
| 4 | `manual__2026-11-09T11:27:11.863311+00:00` | после пересоздания контейнера Airflow (раздел 13) | success, PASSED, **псевдонимы другие** |

Если не сказано иное, цифры относятся к запуску 1. Запуски 1–3 дали одинаковые результаты.

## 1. Цель проекта и что в него не входит

**Цель** (по README и коду): учебный демо-проект. SaaS для отелей хранит витрину активности клиентов
с персональными данными (слой `dwh`). Нужно автоматически построить из неё обезличенную копию (слой `ml`),
из которой модель прогноза оттока берёт данные. Обезличивание сочетает псевдонимизацию ключа с солью,
удаление прямых идентификаторов, маскирование, обобщение квазиидентификаторов и подавление малых групп
(k-анонимность). Результат проверяется автоматически, каждый запуск пишется в журнал. Доступ к слоям
разграничен средствами RBAC ClickHouse.

**Чего в проекте нет** (проверено по коду):

- **Модели оттока нет.** Нет ни кода обучения, ни ML-библиотек, ни признаков сверх витрины `ml.v_ml_client_month`.
  Проект только готовит для неё данные.
- **Реальных данных нет.** Все данные синтетические (`synthetic/generate.py`), и при каждом запуске DAG
  слой `dwh` удаляется и создаётся заново.
- Нет инкрементальной загрузки: каждый раз полная перезаливка (TRUNCATE + INSERT).
- Нет TLS между компонентами: ClickHouse отвечает по HTTP на 8123.
- Нет аутентификации в Airflow: `SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`, вход без пароля.
- Нет ротации соли, нет внешнего хранилища секретов (Vault и т. п.).
- Нет метрик l-разнообразия и t-близости, нет оценки риска по атрибутам факта.
- Нет обезличивания по требованию госорганов (постановление № 1154), нет передачи данных третьим лицам.
- Нет автотестов (pytest и т. п.) и CI. Проверки есть только в задаче `validate_ml`.
- В коде нет комментариев: описание лежит в README.

## 2. Стек, файлы, ресурсы

### 2.1 Версии (фактически запущенные)

| Компонент | Задано в коде | Фактически в запуске |
|---|---|---|
| ClickHouse | образ `clickhouse/clickhouse-server:26.8` | `SELECT version()` → **26.8.36.3** |
| Airflow | образ `apache/airflow:3.4.0-python3.12` | **3.4.0** (`/api/v2/version`), LocalExecutor, `airflow standalone` |
| Python (в контейнере Airflow) | python3.12 | **3.12.16** |
| PostgreSQL (метаданные Airflow) | `postgres:17-alpine` | **17.11** |
| clickhouse-connect | `_PIP_ADDITIONAL_REQUIREMENTS: clickhouse-connect==1.12.0` | **1.12.0** (+ зависимости lz4 4.4.5, backports.zstd 1.8.0) |
| PyYAML | не задан, из образа Airflow | 6.0.3 |
| pendulum | не задан, из образа Airflow | 3.3.0 |
| psycopg2-binary | не задан, из образа Airflow | 2.9.13 |
| apache-airflow-providers-postgres | не задан, из образа Airflow | 7.1.1 |

Контейнеров три: `postgres`, `clickhouse`, `airflow`.

### 2.2 Дерево файлов

```
.env.example                  образец .env: K_MIN, пароли etl_user/ml_user/admin ClickHouse, MASKING_SALT
.gitignore                    исключает .env, __pycache__, *.pyc, logs/
README.md                     описание проекта, схемы, правила, сценарий показа
docker-compose.yml            три сервиса; переменные из .env с ${VAR:?} (без .env compose не стартует);
                              при старте airflow: db migrate, создание Variable masking_salt, airflow standalone
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
masking/sql/ddl_ml.sql        CREATE IF NOT EXISTS таблиц ml, masking_log и CREATE OR REPLACE VIEW v_ml_client_month
masking/sql/dwh_client_qi.sql обобщение квазиидентификаторов и размер группы (оконная функция); общий фрагмент для маскирования и проверок
masking/sql/mask_dim_client.sql  TRUNCATE + INSERT ml.dim_client с псевдонимом, масками, обобщением, фильтром k
masking/sql/copy_dims.sql     TRUNCATE + INSERT ml.dim_tariff, ml.dim_date (копия как есть)
masking/sql/mask_fact.sql     TRUNCATE + INSERT ml.fact_client_activity с псевдонимом, без строк подавленных клиентов
masking/sql/validate_ml.sql   восемь проверок; каждая возвращает ok и details
```

Всего в репозитории 1213 строк (`wc -l` по `git ls-files`).

### 2.3 Требования к памяти и диску

В репозитории требования к памяти **не указаны**: ни в README, ни в compose (лимитов `mem_limit` нет).
Ниже то, что намерено на стенде (`docker stats`, сразу после запусков DAG):

| Контейнер | RAM |
|---|---|
| airflow (standalone: api-server, scheduler, dag-processor, triggerer) | 1,37–1,52 GiB |
| clickhouse | 260–293 MiB (`MemoryResident` ≈ 875 MiB) |
| postgres | ≈ 65 MiB |
| **Итого** | ≈ 1,9 GiB по `docker stats` |

Расхождение у ClickHouse: `docker stats` и внутренняя метрика `MemoryResident` считают память по-разному,
чем именно объясняется разница, не исследовалось.

Образы на диске: airflow 3,25 GB, clickhouse 1,2 GB, postgres 424 MB, итого ≈ 4,9 GB. Данные в ClickHouse:
`dwh` ≈ 292 KiB, `ml` ≈ 219 KiB. На практике стенд работал на 15,7 GiB RAM, а минимальный объём памяти
не проверялся.

## 3. Архитектура

### 3.1 Слои и базы

Обе базы находятся в одном экземпляре ClickHouse, базы и пользователей создаёт `clickhouse/initdb/01_users.sh`:

| База | Комментарий в DDL | Содержимое | Кто имеет доступ |
|---|---|---|---|
| `dwh` | «Слой 1: витрина с персональными данными» | звезда с ПДн | `etl_user` (все права) |
| `ml` | «Слой 2: обезличенная копия для ML» | обезличенная звезда, журнал, витрина | `etl_user` (все права), `ml_user` (только SELECT) |

Метаданные Airflow, включая Variable `masking_salt`, хранятся в отдельном Postgres.

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

Модель должна читать данные под `ml_user`, у которого есть только `SELECT ON ml.*`. Самой модели и кода,
который читает витрину, в репозитории нет (см. раздел 1). Учтите, что `ml_user` видит не только
представление, а все объекты `ml`, включая `ml.dim_client` с масками (см. раздел 13).

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
`connected_quarter LowCardinality(String)`.
Расхождение с README: на схеме в README указан тип `String(16)`, в DDL просто `String`. Фактическая длина
псевдонима во всех строках 16 символов.

**ml.dim_tariff** и **ml.dim_date** по типам совпадают с `dwh`.

**ml.fact_client_activity** совпадает с `dwh`, кроме `client_key String` (псевдоним).
`ORDER BY (client_key, date_key)`.

**ml.masking_log**, `ORDER BY logged_at`: `run_id String`, `logged_at DateTime DEFAULT now()`,
`dim_client_read/written`, `dim_tariff_read/written`, `dim_date_read/written`, `fact_read/written`,
`suppressed_clients`, `suppressed_fact_rows`, `k_min`, `k_actual` (все UInt32),
`checks_status LowCardinality(String)` (PASSED/FAILED), `checks_details String` (JSON).

**ml.v_ml_client_month**: представление (VIEW), см. 3.4.

### 4.3 Генерация синтетики (`synthetic/generate.py`)

Параметры из кода:

- `SEED = 20251101` (`random.Random`), то есть генерация детерминирована;
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

Фактический объём (запуск 1, лог задачи `generate_dwh` и запросы к ClickHouse):

| Таблица | Строк |
|---|---|
| dwh.dim_client | 1000 |
| dwh.dim_tariff | 4 |
| dwh.dim_date | 24 |
| dwh.fact_client_activity | **19 928** |

Ещё по фактическим данным `dwh`:

- строк факта на клиента: минимум 1, медиана 23, максимум 24, среднее 19,93;
- диапазон `date_key`: 2024-11-01 … 2026-10-01; во всех 1000 клиентов есть строки факта;
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
| 1 | `generate_dwh` | `synthetic.generate.generate()`: DROP + CREATE таблиц `dwh`, вставка синтетики, проверка сирот и зерна | 6,48 с | 1,41 с |
| 2 | `mask_dim_client` | проверка соли, сверка колонок `dwh` с `rules.yaml` (`check_rules_cover_schema`), `ddl_ml.sql`, `mask_dim_client.sql`; статистика и фактическое k | 0,36 с | 0,40 с |
| 3 | `copy_dims` | `copy_dims.sql`: TRUNCATE + копия `dim_tariff`, `dim_date` | 0,29 с | 0,30 с |
| 4 | `mask_fact` | `mask_fact.sql`: TRUNCATE + факт с псевдонимом, без строк подавленных клиентов | 0,28 с | 0,32 с |
| 5 | `validate_ml` | 8 проверок из `validate_ml.sql`; результат пишется в XCom `checks`; при любом FAIL задача падает | 0,41 с | 0,47 с |
| 6 | `write_log` | `trigger_rule=ALL_DONE`; собирает статистику из XCom трёх задач и `checks`, пишет строку в `ml.masking_log`; падает, если статус FAILED | 0,79 с | 0,91 с |

Время задачи считается от `start_date` до `end_date` экземпляра задачи. Весь запуск 1 длился 13,08 с
(11:22:23.42 → 11:22:36.50), запуск 2 занял 8,13 с. Почему `generate_dwh` в первом запуске шёл дольше,
не исследовалось.

**Повторная запускаемость** (по коду):

- `dwh` пересоздаётся (DROP + CREATE), генерация с фиксированным seed, поэтому данные каждый раз одинаковые;
- таблицы `ml` создаются через `CREATE TABLE IF NOT EXISTS`, представление через `CREATE OR REPLACE VIEW`;
- перед каждой вставкой в `ml.dim_client`, `ml.dim_tariff`, `ml.dim_date`, `ml.fact_client_activity`
  выполняется `TRUNCATE`, поэтому повтор не создаёт дублей;
- псевдоним детерминирован при той же соли;
- `ml.masking_log` не очищается и копит по строке на каждый запуск;
- `max_active_runs=1` не даёт двум запускам выполняться одновременно.

Подтверждено запуском 2: в `ml` снова 805 клиентов и 16 045 строк факта, дублей по
`(client_key, date_key)` 0, MD5 отсортированного списка псевдонимов совпал с запуском 1
(`1ecf06dd…`), в `masking_log` добавилась вторая строка.
**Оговорка:** стабильность сохраняется, только пока жив контейнер Airflow (см. раздел 13).

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
- Без соли перебрать 1000 исходных ключей бесполезно. Если соль известна, перебор 1000 ключей обращает
  псевдоним сразу (см. раздел 13).
- Почему SHA-256: в README приведён замер «на стенде» на 10 млн строк (SHA-256 1,3 с, `sipHash64` 0,33 с,
  `cityHash64` 0,14 с). **В этом запуске он не повторялся**, это цифры из README.

### 6.3 Где хранится соль и как она не попадает в SQL, логи и git

По коду и конфигурации:

1. **Источник.** `MASKING_SALT` в `.env`. Если переменная пустая, при старте контейнера Airflow генерируется
   `secrets.token_hex(32)` (64 hex-символа) (`docker-compose.yml`, `command`). В этом запуске переменная
   была пустой, соль сгенерирована, длина 64.
2. **Хранение.** Airflow Variable `masking_salt` в базе метаданных Postgres. В таблице `variable` стоит
   `is_encrypted = t`, то есть значение зашифровано Fernet. Ключ Fernet лежит в `/opt/airflow/airflow.cfg`
   внутри контейнера, этот путь **не вынесен в volume** (последствия в разделе 13). После записи
   Variable shell выполняет `unset MASKING_SALT`, но в `docker inspect` переменная видна в окружении
   контейнера. В этом запуске она была пустой, потому что значение не задавалось.
3. **git.** `.env` указан в `.gitignore`. Поиск значения соли по рабочей копии без `.git` нашёл 0 файлов.
4. **SQL-файлы.** Соль передаётся только как параметр `{salt:String}` (`PSEUDO_KEY` в `masker.py`)
   через `parameters=` в clickhouse-connect. В текстах SQL её нет.
5. **system.query_log ClickHouse.** Для запросов с параметром `salt` `db.run_sql` выставляет
   `log_queries=0, log_query_threads=0, log_query_views=0`. Проверено после `SYSTEM FLUSH LOGS`:
   строк `query_log` со значением соли 0. Запросов маскирования с `SHA256` в `query_log` тоже нет:
   нашлись только два `CREATE USER … sha256_password` из initdb, и пароли в них ClickHouse скрыл.
   От `etl_user` за период запусков в журнале 187 записей `QueryFinish`, то есть остальные запросы
   логируются как обычно.
6. **Логи сервера ClickHouse.** Уровень `information` (`clickhouse/config/logging.xml`). Поиск соли
   в `/var/log/clickhouse-server/` дал 0 файлов, в `system.text_log` 0 строк.
7. **Логи Airflow.** `AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES=salt`, поэтому Variable с `salt` в имени
   маскируется в логах задач. Поиск соли по `/opt/airflow/logs` дал 0 файлов. В `docker compose logs`
   всех трёх сервисов 0 совпадений.
8. **Таблицы ml.** В `ml.masking_log.checks_details` и `ml.dim_client` соли нет (0 совпадений).
   Задача `validate_ml` соль не использует.

Что не проверялось: показывает ли UI или REST API Airflow значение Variable `masking_salt`. Проверено
только, что CLI `airflow variables get masking_salt` внутри контейнера возвращает его в открытом виде.

## 7. k-анонимность

- **K_MIN = 5.** Источник: `.env` (`K_MIN=5`, как в `.env.example`) → `AIRFLOW_VAR_K_MIN` → Variable `k_min`.
  Значение по умолчанию в коде (`_k_min`) тоже 5.
- **Квазиидентификаторы:** `federal_district`, `rooms_range`, `connected_quarter`
  (`rules.yaml`, `k_anonymity.quasi_identifiers`, действие `on_violation: suppress`).
- **Расчёт по клиентам**, а не по строкам факта: в `dwh_client_qi.sql` считается
  `count() OVER (PARTITION BY federal_district, rooms_range, connected_quarter) AS group_size` по строкам
  `dwh.dim_client`, одна строка на клиента. Фактическое k в `ml`: `min(count())` по группам `ml.dim_client`
  (`masker.mask_dim_client`, проверка `k_anonymity`).
- **Подавленные клиенты** (`group_size < K_MIN`) целиком не попадают в `ml.dim_client`, и **все их строки
  факта** тоже не попадают в `ml.fact_client_activity`. Других вариантов (переобобщения, слияния групп)
  в коде нет.

Результат запуска 1:

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

Как подавление меняет распределение клиентов (dwh → ml):

| Федеральный округ | dwh | ml |
|---|---|---|
| Центральный | 363 | 357 |
| Южный | 210 | 186 |
| Северо-Западный | 184 | 160 |
| Приволжский | 114 | 81 |
| Уральский | 43 | 5 |
| Сибирский | 34 | 10 |
| Дальневосточный | 31 | 6 |
| Северо-Кавказский | 21 | **0** |

| rooms_range | dwh | ml | | connected_quarter | dwh | ml |
|---|---|---|---|---|---|---|
| до 20 | 429 | 376 | | 2024-Q1 | 210 | 174 |
| 21–50 | 334 | 289 | | 2024-Q2 | 219 | 186 |
| 51–100 | 157 | 104 | | 2024-Q3 | 183 | 152 |
| 100+ | 80 | 36 | | 2024-Q4 | 151 | 112 |
| | | | | 2025-Q1 | 139 | 115 |
| | | | | 2025-Q2 | 98 | 66 |

Подавление неравномерно. Малые округа почти исчезают, Северо-Кавказский пропадает полностью, крупные отели
(100+) теряют 55 %. Для модели оттока это смещение выборки.

## 8. Разграничение доступа

Пользователей создаёт `clickhouse/initdb/01_users.sh`, аутентификация `sha256_password`. Пароли берутся
из `.env`, в отчёт они не включены.

| Пользователь | Назначение | Права (`SHOW GRANTS`, фактически) |
|---|---|---|
| `admin` | администратор (`CLICKHOUSE_USER`, `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`) | все, управление доступом |
| `etl_user` | Airflow: генерация, маскирование, проверки | `GRANT ALL ON dwh.*`, `GRANT ALL ON ml.*` (ClickHouse раскрывает их в полный список: SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, …) |
| `ml_user` | потребитель обезличенных данных (модель, аналитик) | `GRANT SELECT ON ml.* TO ml_user` |

Реальный вывод под `ml_user` (`clickhouse-client`, запуск 1).

Обращение к исходному слою:

```
SELECT * FROM dwh.dim_client LIMIT 5

Received exception from server (version 26.8.36):
Code: 497. DB::Exception: Received from localhost:9000. DB::Exception: ml_user: Not enough privileges.
To execute this query, it's necessary to have the grant SELECT ON dwh.dim_client. (ACCESS_DENIED)
```

Обращение к обезличенному слою:

```
SELECT client_key, federal_district, rooms_range, connected_quarter FROM ml.dim_client ORDER BY client_key LIMIT 3

   ┌─client_key───────┬─federal_district─┬─rooms_range─┬─connected_quarter─┐
1. │ 0011a7272d13ff17 │ Центральный      │ до 20       │ 2025-Q2           │
2. │ 00529f89aa8b5d81 │ Центральный      │ до 20       │ 2024-Q2           │
3. │ 00a01c9f149c7c28 │ Центральный      │ 21–50       │ 2024-Q1           │
   └──────────────────┴──────────────────┴─────────────┴───────────────────┘
```

Остальные проверки под `ml_user`:

- `SHOW DATABASES` возвращает только `ml`;
- `INSERT INTO ml.dim_client …` даёт `Code: 497 … necessary to have the grant INSERT(client_key) ON ml.dim_client. (ACCESS_DENIED)`;
- `SELECT count() FROM system.query_log` даёт `Code: 497 … SELECT ON system.query_log. (ACCESS_DENIED)`.

Ограничение: `ml_user` может читать все объекты `ml`, а не только `v_ml_client_month`. Это `ml.dim_client`
с `email_masked`, `phone_masked` и `ml.masking_log`. Row policy и права на отдельные колонки не настроены.

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

В запусках 1–4 все восемь проверок OK, статус PASSED.

**Негативный тест (выполнен).** В `ml.dim_client` вставлена строка клиента 1 с исходным ключом `'1'`
и исходным email. Затем `masker.validate(5)` вызван вручную внутри контейнера Airflow:

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

Это совпадает с README («падает на `clients_balance`, `no_source_keys`, `no_pii`»). Тест запускался
не через DAG, поэтому в `masking_log` строки FAILED нет. Ветку `write_log` со статусом FAILED
**через DAG не проверяли**. Слой `ml` восстановлен запуском 3.

Чего проверки не покрывают: в `no_pii` проверяется только `ml.dim_client`, факт на PII не проверяется
(PII-колонок в нём нет по DDL). Уникальность масок и рядов активности не проверяется (см. раздел 13).

## 10. Результаты запуска в цифрах

**run_id:** `manual__2026-11-09T11:22:22.783054+00:00`. Состояние `success`,
старт 11:22:23.421, конец 11:22:36.501 UTC (13,08 с). Время задач приведено в разделе 5.

| Таблица | Прочитано (dwh) | Записано (ml) | Не перенесено |
|---|---|---|---|
| dim_client | 1000 | 805 | 195 (подавлены) |
| dim_tariff | 4 | 4 | 0 |
| dim_date | 24 | 24 | 0 |
| fact_client_activity | 19 928 | 16 045 | 3 883 (строки подавленных) |

- Подавлено клиентов: **195 (19,5 %)**, строк факта: **3 883 (19,5 %)**.
- Фактическое k: **5** при K_MIN = 5.
- Группы в ml: **65**, размер мин / медиана / макс: **5 / 11 / 39**.
- Витрина `ml.v_ml_client_month`: 16 045 строк, 805 псевдонимов, `sum(churn_flag)` = 223.

Содержимое `ml.masking_log` после запуска 1 (одна строка):

| Поле | Значение |
|---|---|
| run_id | manual__2026-11-09T11:22:22.783054+00:00 |
| logged_at | 2026-11-09 11:22:35 |
| dim_client_read / written | 1000 / 805 |
| dim_tariff_read / written | 4 / 4 |
| dim_date_read / written | 24 / 24 |
| fact_read / written | 19928 / 16045 |
| suppressed_clients | 195 |
| suppressed_fact_rows | 3883 |
| k_min / k_actual | 5 / 5 |
| checks_status | PASSED |
| checks_details | JSON-массив из 8 объектов `{check, description, ok, details}`; значения `details` совпадают с колонкой «Факт» в разделе 9 |

После всех запусков в журнале четыре строки (запуски 1–4), у всех одинаковые счётчики и статус PASSED.

Для сравнения со «Сценарием показа» в README: заявленные там 805 клиентов, 16 045 строк, 195 подавленных,
3 883 строки и k = 5 совпали с фактом.

## 11. Примеры (синтетические данные)

### 11.1 До и после маскирования: dim_client

Пять записей `dwh.dim_client` (клиенты 1–5) и соответствующие строки `ml.dim_client`. Сопоставление
по вычисленному псевдониму сделано под `etl_user` с `log_queries=0`, псевдонимы из запусков 1–3
(до пересоздания контейнера, см. раздел 13). Клиент 24 используется в `checks.sql`.

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
| 1 | 261475fe8302b303 | v***@list.ru | +7 *** *** 46 42 | Центральный | 100+ | 2024-Q3 |
| 2 | d71bbb0ccb41c089 | v***@list.ru | +7 *** *** 44 72 | Центральный | 21–50 | 2025-Q2 |
| 3 | f6eaecd1a1d2b308 | y***@mail.ru | +7 *** *** 75 40 | Центральный | 21–50 | 2024-Q4 |
| 4 | 4aa60f0060060ce7 | k***@mail.ru | +7 *** *** 76 18 | Центральный | 21–50 | 2025-Q1 |
| 5 | 0fb72217eff114f0 | p***@yandex.ru | +7 *** *** 37 83 | Центральный | 51–100 | 2024-Q2 |
| 24 | e891fbe5993a05fe | o***@gmail.com | +7 *** *** 13 51 | Южный | 21–50 | 2024-Q1 |

Колонка «исх. ключ» добавлена только для отчёта, в `ml` её нет. Примеры подавленных клиентов: 12
(Санкт-Петербург, 270 номеров, 2024-05-02, 24 строки факта), 40 (Хабаровск, 20, 2025-02-10, 20 строк),
44 (Новосибирск, 76, 2025-02-28, 12 строк). Ни их, ни их строк факта в `ml` нет: поиск по их псевдонимам
в `ml.fact_client_activity` вернул 0.

### 11.2 Итоговая витрина для ML

`SELECT * FROM ml.v_ml_client_month ORDER BY client_key, date_key LIMIT 5` под `ml_user`, запуск 3:

| client_key | date_key | year | quarter | month | federal_district | rooms_range | connected_quarter | tariff_key | tariff_name | tariff_price | logins_count | active_users | bookings_processed | features_used | tickets_count | late_payments | mrr | churn_flag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0011a7272d13ff17 | 2025-05-01 | 2025 | 2 | 5 | Центральный | до 20 | 2025-Q2 | 2 | Бизнес | 9900 | 12 | 1 | 8 | 9 | 4 | 0 | 9900 | 0 |
| 0011a7272d13ff17 | 2025-06-01 | 2025 | 2 | 6 | Центральный | до 20 | 2025-Q2 | 2 | Бизнес | 9900 | 8 | 1 | 5 | 9 | 2 | 0 | 9900 | 0 |
| 0011a7272d13ff17 | 2025-07-01 | 2025 | 3 | 7 | Центральный | до 20 | 2025-Q2 | 2 | Бизнес | 9900 | 13 | 1 | 6 | 10 | 1 | 1 | 9900 | 0 |
| 0011a7272d13ff17 | 2025-08-01 | 2025 | 3 | 8 | Центральный | до 20 | 2025-Q2 | 2 | Бизнес | 9900 | 8 | 1 | 6 | 10 | 4 | 0 | 9900 | 0 |
| 0011a7272d13ff17 | 2025-09-01 | 2025 | 3 | 9 | Центральный | до 20 | 2025-Q2 | 2 | Бизнес | 9900 | 8 | 1 | 8 | 10 | 3 | 1 | 9900 | 0 |

Отток по округам из витрины (запрос 2.2 из `checks.sql`): Центральный 357 клиентов / 91 ушёл (0,255),
Южный 186 / 56 (0,301), Северо-Западный 160 / 44 (0,275), Приволжский 81 / 29 (0,358), Сибирский 10 / 0 (0),
Дальневосточный 6 / 1 (0,167), Уральский 5 / 2 (0,4).

## 12. Соответствие приказу Роскомнадзора от 19.06.2025 № 140

**Ограничение раздела.** Официальный текст приказа получить не удалось: сайты с текстом (consultant.ru,
pravo.ppt.ru) недоступны из сети стенда. Поэтому **дословных формулировок из приказа здесь нет**.
Использованы только вторичные источники, найденные поиском:

- приказ утверждает требования к обезличиванию и методы обезличивания, зарегистрирован в Минюсте 31.07.2025
  № 83110, вступил в силу 01.09.2025 и отменил приказ РКН № 996 от 05.09.2013;
- методы можно применять по одному или комбинировать;
- в пересказах требований (1С:ИТС): заранее определить состав ПДн и субъектов, оценить достаточность метода,
  обеспечить невозможность определить субъекта без дополнительной информации, исключить совместное хранение
  обезличенного массива с исходным и дополнительной информацией, вести учёт действий по обезличиванию;
- число методов источники называют по-разному (четыре или пять), их названия в найденных фрагментах
  полностью не приведены.

Для статьи формулировки и номера пунктов нужно сверить с официальной публикацией
(publication.pravo.gov.ru или КонсультантПлюс).

Сопоставление ниже составлено мной по общепринятым названиям методов (введение идентификаторов, изменение
состава или семантики, декомпозиция, перемешивание; эти названия были в приказе № 996). Входят ли они
в приказ № 140 в той же формулировке, **не проверено**.

| Поле / мера в проекте | Применённый метод (в проекте) | Предполагаемый метод по классификации РКН | Точность соответствия |
|---|---|---|---|
| client_key | замена на псевдоним SHA-256 с солью; «дополнительная информация» (соль) хранится отдельно, в Airflow | введение идентификаторов | вероятное; формулировка из № 140 не сверена. В классическом методе ведётся таблица соответствия, здесь её нет, сопоставление восстанавливается вычислением при известной соли |
| contact_name, hotel_name, inn | удаление атрибутов | изменение состава (сокращение перечня обрабатываемых сведений) | вероятное; не сверено |
| email, phone | частичное маскирование | изменение семантики / состава | не сверено; отдельного метода «маскирование» в известных мне формулировках нет |
| city → федеральный округ, rooms_count → диапазон, connected_date → квартал | обобщение | изменение семантики (замена значения более общим) | вероятное; не сверено |
| подавление групп < K_MIN | удаление записей | точного соответствия не найдено; k-анонимность как критерий в найденных пересказах не упоминается | **нет точного соответствия** |
| dim_tariff, dim_date, показатели факта | без изменений | — | обезличивание не применялось |
| — | декомпозиция, перемешивание | в проекте не применяются | — |

Сопоставление с требованиями (по пересказу 1С, не по тексту приказа):

| Требование (пересказ) | Что есть в проекте | Оценка |
|---|---|---|
| Определить состав ПДн до обезличивания | `rules.yaml` с правилом на каждое поле; `check_rules_cover_schema` не пропускает колонку без правила | реализовано технически; формального документа (перечня, акта) нет |
| Оценить достаточность метода | проверки `validate_ml`, фактическое k | частично: не оценивается риск по атрибутам факта и по маскам (раздел 13) |
| Невозможность определить субъекта без доп. информации | псевдоним без соли не обращается | частично: без соли возможна реидентификация через внешние данные (раздел 13) |
| Раздельное хранение обезличенного и исходного массива и доп. информации | `dwh` и `ml` — разные базы с разными правами; соль в Postgres Airflow | частично: обе базы в одном экземпляре ClickHouse, `etl_user` имеет доступ к обеим |
| Учёт действий по обезличиванию | `ml.masking_log`: строка на запуск со счётчиками и результатами проверок | реализовано для запусков DAG; ручные действия не журналируются |

## 13. Ограничения и слабые места

Ниже факты, проверенные запросами на данных запуска 1 (805 клиентов в `ml`), если не сказано иное.

### 13.1 Риски реидентификации

1. **Маски email и телефона почти уникальны.** В `ml.dim_client`:
   - `phone_masked`: 771 разное значение. У **738 из 805** клиентов (91,7 %) маска телефона уникальна:
     последние 4 цифры почти однозначно указывают на клиента;
   - `email_masked`: 78 разных значений, уникально только у 1 клиента;
   - пара (`email_masked`, `phone_masked`) уникальна у **всех 805**;
   - (квазиидентификаторы + `email_masked`) уникальны у 627 клиентов.

   k-анонимность считается только по трём квазиидентификаторам, маски в расчёт не входят. Тот, у кого
   есть телефоны клиентов (CRM, справочник), восстанавливает связь «псевдоним → клиент» для большинства
   записей. Маски доступны `ml_user` (`SELECT ON ml.*`), хотя в `v_ml_client_month` их нет.
2. **Ряды активности уникальны.** В `ml.fact_client_activity`:
   - помесячный ряд `logins_count` уникален у **805 из 805** клиентов;
   - ряд `mrr` и ряд тарифов уникальны у 195 клиентов (24 %);
   - тройка (первый месяц, последний месяц, был ли уход) уникальна у 49 клиентов;
   - даже в одном месяце (2026-01) пара (`logins_count`, `bookings_processed`) уникальна у 563 строк из 693.

   Уникальный ряд сам по себе не раскрывает личность, но позволяет связать записи с любым внешним
   источником, где есть тот же ряд или его часть (выгрузка биллинга, другая копия данных). Сам проект
   использует это в `checks.sql` 1.3: клиент находится в `ml` по совпадению ряда. k-анонимность защищает
   только измерение и на факт не распространяется.
3. **Стабильный псевдоним.** При той же соли псевдоним одинаков во всех запусках, поэтому выгрузки разных
   периодов связываются между собой и с уже реидентифицированными записями.
4. **Кто знает соль, обращает псевдонимы.** Ключи — это 1…1000, и перебор занимает миллисекунды. Соль
   доступна всем, у кого есть доступ к Airflow: `airflow variables get masking_salt` в контейнере
   возвращает её открытым текстом (проверено), а вход в UI Airflow без пароля. Доступ к значению через UI
   и REST API не проверялся.
5. **Усечение до 64 бит.** Коллизий на 805 значениях нет (проверено), на больших объёмах не оценивалось.
6. **Нет l-разнообразия и t-близости.** В группах может совпадать `churn_flag` или тариф. Не проверялось.
7. **Смещение выборки.** Подавлено 19,5 % клиентов, неравномерно: Северо-Кавказский округ пропал полностью,
   крупные отели потеряли 55 % (раздел 7). Как это влияет на качество модели оттока, не оценивалось.

### 13.2 Слабые места реализации

1. **Соль теряется при пересоздании контейнера Airflow (подтверждено опытом).** Variable `masking_salt`
   зашифрована Fernet-ключом из `/opt/airflow/airflow.cfg`. Этот файл лежит в файловой системе контейнера,
   а база Postgres хранится в volume. После `docker compose up -d --force-recreate airflow` хэш значения
   `airflow variables get masking_salt` изменился. Старое значение, по-видимому, не расшифровалось, и
   скрипт старта записал новую соль. Запуск 4 прошёл со статусом **PASSED**, но MD5 списка псевдонимов
   стал другим (`d1922cfe…` вместо `1ecf06dd…`). То есть утверждение README «псевдоним стабилен между
   запусками» верно только в рамках одного контейнера, и проверки смену псевдонимов не обнаруживают.
   Механизм (ошибка расшифровки → ветка `elif`) выведен из кода и результата. Сообщение об ошибке
   в логах не искалось, потому что вывод этих команд в `command` перенаправлен в `/dev/null`.
2. Если `MASKING_SALT` задан в `.env`, он виден в окружении контейнера (`docker inspect`, `/proc/1/environ`):
   `unset` действует только на shell-процесс. Это следует из конфигурации, в запуске соль не задавалась.
3. `log_queries=0` выставляется только в `db.run_sql`. Любой ручной запрос с солью в тексте (например,
   администратор вычисляет псевдоним вручную) попадёт в `system.query_log`. При подготовке отчёта мои
   служебные поисковые запросы под `admin` содержали значение соли (первой, до пересоздания) и попали
   в `query_log` стенда. К коду проекта это не относится, но показывает границу защиты.
4. `etl_user` имеет `ALL` на обе базы. Это одна учётная запись с доступом и к ПДн, и к обезличенным данным,
   а её пароль лежит в окружении контейнера Airflow.
5. `dwh` и `ml` находятся в одном экземпляре ClickHouse: разделение логическое (RBAC), физически хранилище одно.
6. Нет TLS (HTTP 8123), порты 8123 и 8080 опубликованы на хосте.
7. Нет row policy и прав на колонки: `ml_user` видит `ml.dim_client` с масками и `ml.masking_log`.

### 13.3 Что не проверялось

- текст приказа № 140 (раздел 12);
- работа на объёмах больше 1000 клиентов, производительность (замер SHA-256 из README не повторялся);
- ветка `write_log` со статусом FAILED через DAG; падение `check_rules_cover_schema` на колонке без правила;
  поведение при неизвестном городе (в синтетике таких 0);
- значения `K_MIN`, отличные от 5;
- атаки с внешними данными (связывание по телефону или ряду MRR выполнено только как подсчёт уникальности,
  атака не моделировалась);
- доступ к соли через UI и REST API Airflow;
- минимальные требования к памяти;
- обучение модели оттока на `ml` и сравнение с моделью на `dwh`.

### 13.4 Отличия от реального внедрения

- данные синтетические и детерминированные, `dwh` пересоздаётся генератором; в реальности `dwh` — источник,
  его нельзя DROP;
- полная перезаливка вместо инкремента;
- соль в Airflow Variable без внешнего хранилища секретов и без ротации;
- Airflow `standalone` без аутентификации — режим разработки (в логе контейнера: «This is a development/test
  feature only. NEVER use it in production!»);
- нет организационных мер: приказа об обезличивании, назначенных лиц, оценки достаточности, журнала
  ручных действий.

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

Остановить с удалением данных:

```bash
docker compose down -v
```

Не делайте `docker compose up -d --force-recreate` и не меняйте compose между показами без `down -v`:
соль сменится, и псевдонимы станут другими (раздел 13.2.1).

### 14.2 Порядок демонстрации

1. Airflow → DAG `mask_dwh_to_ml` → **Trigger**. Во вкладке **Graph** шесть задач, все зелёные
   за ≈ 10–15 с (в запусках 1–2: 13,1 с и 8,1 с).
2. Лог задачи `validate_ml`: восемь строк `[OK  ]` с цифрами из раздела 9.
3. ClickHouse Play под `etl_user`: запросы 1.1–1.3 из `checks.sql`. Клиент 24 в `dwh` с ПДн, его
   помесячная активность, тот же клиент в `ml` (псевдоним, `o***@gmail.com`, `+7 *** *** 13 51`,
   Южный / 21–50 / 2024-Q1). Найти его удаётся только по совпадению ряда активности, и это заодно
   иллюстрирует риск из раздела 13.1 п. 2.
4. Под `ml_user`: 2.1–2.2 (витрина для ML и отток по округам), 3.1–3.3 (фактическое k, малые группы,
   журнал), 4.1 (`ACCESS_DENIED`).
5. Повторный **Trigger**: в `ml` те же 805 / 16 045, псевдонимы те же, в `masking_log` новая строка.

### 14.3 Запросы для ClickHouse Play

Play выполняет всё поле как один запрос, поэтому вставляйте по одному. Полный набор лежит в `checks.sql`,
ниже основные.

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

Под `ml_user`:

```sql
SELECT * FROM ml.v_ml_client_month ORDER BY client_key, date_key LIMIT 20;
```

```sql
SELECT min(clients) AS k_actual, count() AS groups, sum(clients) AS clients_total
FROM (SELECT federal_district, rooms_range, connected_quarter, count() AS clients
      FROM ml.dim_client GROUP BY federal_district, rooms_range, connected_quarter);
```

```sql
SELECT run_id, logged_at, dim_client_read, dim_client_written, fact_read, fact_written,
       suppressed_clients, suppressed_fact_rows, k_min, k_actual, checks_status
FROM ml.masking_log ORDER BY logged_at DESC;
```

```sql
SELECT * FROM dwh.dim_client LIMIT 5;   -- ожидается Code: 497 ACCESS_DENIED
```

Запросы для раздела о рисках, под `ml_user` (их нет в `checks.sql`):

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
