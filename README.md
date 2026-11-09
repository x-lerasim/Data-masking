# Маскирование персональных данных между слоями ClickHouse

Учебный демо-проект. SaaS для отелей: витрина активности клиентов с персональными
данными (слой `dwh`) и её обезличенная копия (слой `ml`), из которой модель
прогноза оттока берёт данные. Оркестрация — Airflow, все данные синтетические.

Стек: ClickHouse 26.8, Airflow 3.4.0 (LocalExecutor, `airflow standalone`),
Postgres 17 для метаданных Airflow, clickhouse-connect. Три контейнера.

## Схема

```
                     Airflow DAG mask_dwh_to_ml (ручной запуск)
 generate_dwh → mask_dim_client → copy_dims → mask_fact → validate_ml → write_log
      │               │               │            │            │            │
      ▼               ▼               ▼            ▼            ▼            ▼
┌──────────────── ClickHouse ──────────────────────────────────────────────────────┐
│  dwh  (слой 1, ПДн)                 ml  (слой 2, обезличено)                       │
│  доступ: etl_user                   доступ: etl_user (всё), ml_user (SELECT только │
│                                     v_ml_client_month и masking_log)              │
│                                                                                   │
│  dim_client ──── псевдоним, маски, ──────► dim_client                             │
│                  обобщение, k-анонимность                                         │
│  dim_tariff ──── копия ──────────────────► dim_tariff                             │
│  dim_date   ──── копия ──────────────────► dim_date                               │
│  fact_client_activity ── псевдоним, ─────► fact_client_activity                   │
│                          подавление        v_ml_client_month  ← источник для ML    │
│                                            masking_log        ← журнал запусков    │
└───────────────────────────────────────────────────────────────────────────────────┘
```

Звезда (одинакова в обоих слоях, меняется только `dim_client` и тип `client_key`):

```
                 dim_date                          dim_tariff
             ┌──────────────┐                  ┌──────────────┐
             │ date_key  PK │                  │ tariff_key PK│
             │ year         │                  │ tariff_name  │
             │ quarter      │                  │ price        │
             │ month        │                  └──────┬───────┘
             └──────┬───────┘                         │
                    │      fact_client_activity       │
                    │   (зерно: клиент × месяц)       │
                    │  ┌───────────────────────────┐  │
                    └──┤ date_key    FK            ├──┘
                       │ tariff_key  FK            │
                    ┌──┤ client_key  FK            │
                    │  │ logins_count, active_users│
                    │  │ bookings_processed        │
                    │  │ features_used             │
                    │  │ tickets_count             │
                    │  │ late_payments, mrr        │
                    │  │ churn_flag                │
                    │  └───────────────────────────┘
                    │
   dwh.dim_client   │                      ml.dim_client
 ┌──────────────────┴─┐                  ┌──────────────────────────┐
 │ client_key  UInt32 │  ── SHA-256 ──►  │ client_key  String(16)   │
 │ contact_name       │  ── удалено      │                          │
 │ email              │  ── маска ────►  │ email_masked             │
 │ phone              │  ── маска ────►  │ phone_masked             │
 │ inn                │  ── удалено      │                          │
 │ hotel_name         │  ── удалено      │                          │
 │ city               │  ── обобщение ►  │ federal_district    (КИ) │
 │ rooms_count        │  ── обобщение ►  │ rooms_range         (КИ) │
 │ connected_date     │  ── обобщение ►  │ connected_quarter   (КИ) │
 └────────────────────┘                  └──────────────────────────┘
                                          КИ — квазиидентификатор
```

Синтетика: 1000 клиентов, 24 месяца (2024-11 … 2026-10), ≈ 20 000 строк факта.
Генератор детерминирован (фиксированный seed), ссылочную целостность и зерно
факта проверяет `generate_dwh` — ClickHouse внешние ключи не контролирует.

## Запуск

Нужен Docker с Compose v2. Без `.env` compose не запустится.
В `.env.example` лежит демонстрационный `FERNET_KEY`; для чего-либо кроме демо
сгенерируйте свой (`python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)
и не меняйте его, пока жив volume `pg_data` (см. «Соль»).

```bash
cp .env.example .env
```

```bash
docker compose up -d
```

Через 1–2 минуты (при старте Airflow доустанавливает clickhouse-connect):

- Airflow: http://localhost:8080 — вход без пароля;
- ClickHouse Play: http://localhost:8123/play — `etl_user` / `etl_pass`, `ml_user` / `ml_pass`.

Дальше всё в UI Airflow: DAG `mask_dwh_to_ml` → **Trigger**.

Остановить и удалить данные: `docker compose down -v`.

## Сценарий показа

1. **Airflow** → http://localhost:8080 → DAG `mask_dwh_to_ml` → кнопка **Trigger**.
2. Вкладка **Graph**: шесть задач в цепочке
   `generate_dwh → mask_dim_client → copy_dims → mask_fact → validate_ml → write_log`,
   все зелёные за ~15 секунд. В логе `validate_ml` — восемь строк `[OK]` с цифрами.
3. **ClickHouse Play** → http://localhost:8123/play, запросы из [`checks.sql`](checks.sql)
   по одному (Play выполняет всё поле как один запрос):
   - 1.1–1.3 под `etl_user`: клиент №24 в `dwh` с ФИО, телефоном, ИНН — и тот же клиент
     в `ml`: псевдоним, `o***@gmail.com`, `+7 *** *** 13 51`, «Южный / 21–50 / 2024-Q1»,
     помесячная активность совпадает до цифры. Найти его в `ml` удаётся только
     сравнением ряда активности — соли у аналитика нет;
   - 2.1–2.2 под `ml_user`: `ml.v_ml_client_month` — данные для модели оттока;
   - 3.1–3.2 под `etl_user`: фактическое k и малые группы по `ml.dim_client`;
   - 3.3 под `ml_user`: журнал `ml.masking_log`;
   - 4.1–4.4 под `ml_user`: права только на `v_ml_client_month` и `masking_log`,
     `dwh.dim_client`, `ml.dim_client`, `ml.fact_client_activity` → `ACCESS_DENIED`.
4. Повторный **Trigger**: в `ml` те же 805 клиентов и 16 045 строк факта (без дублей),
   псевдонимы те же, в `masking_log` ещё одна строка. Псевдонимы сохраняются и после
   `docker compose up -d --force-recreate airflow`.

## Структура

```
docker-compose.yml        postgres + clickhouse + airflow (standalone, LocalExecutor)
.env.example              пароли, K_MIN, FERNET_KEY, MASKING_SALT
clickhouse/initdb/        базы dwh, ml и пользователи etl_user, ml_user (RBAC)
clickhouse/config/        уровень логов information (см. «Соль»)
dags/mask_dwh_to_ml.py    DAG: только вызывает функции модулей
synthetic/                DDL слоя dwh и генератор синтетики
masking/
  rules.yaml              правила по полям, квазиидентификаторы, справочники
  masker.py               сборка параметров, запуск SQL, статистика, журнал
  init_salt.py            создание или проверка Variable masking_salt при старте Airflow
  experiment.py           эксперимент K_MIN 3/5/10, квартал/полугодие
  db.py                   clickhouse-connect, выполнение SQL-файлов
  sql/ddl_ml.sql          таблицы ml, masking_log, v_ml_client_month
  sql/dwh_client_qi.sql   квазиидентификаторы и размер группы (общая формула)
  sql/mask_dim_client.sql
  sql/copy_dims.sql
  sql/mask_fact.sql
  sql/validate_ml.sql     восемь проверок
  sql/experiment_k.sql    клиенты dwh с КИ, числом строк факта и оттоком
experiments/              результат эксперимента (md, csv)
checks.sql                запросы для показа
```

## Правила маскирования

| Поле `dwh.dim_client` | В `ml` | Как |
|---|---|---|
| `client_key` | `client_key` | первые 8 байт `SHA-256(соль ‖ ':' ‖ client_key)` в hex. Стабилен, пока не сменились соль и `FERNET_KEY`; без соли не восстанавливается |
| `contact_name`, `hotel_name`, `inn` | — | не выбираются |
| `email` | `email_masked` | `o***@gmail.com` |
| `phone` | `phone_masked` | `+7 *** *** 13 51` |
| `city` | `federal_district` | справочник `city_to_district` в `rules.yaml` |
| `rooms_count` | `rooms_range` | до 20 / 21–50 / 51–100 / 100+ |
| `connected_date` | `connected_quarter` | `2024-Q1` |

`dim_tariff`, `dim_date` копируются как есть. В факте меняется только `client_key`
(тот же псевдоним), показатели и `churn_flag` не трогаются.

`masker.check_rules_cover_schema` сверяет колонки `dwh` с `rules.yaml`: колонка без
правила останавливает DAG, а не уезжает в `ml` по умолчанию.

### Соль

Соль — случайная секретная строка, которая подмешивается к `client_key` перед
хэшированием. Без неё псевдоним легко обратить: ключей всего 1000, хэши всех чисел
1…1000 перебираются за миллисекунды. С солью перебор бесполезен, а одинаковый
ключ при той же соли всегда даёт тот же псевдоним — поэтому он стабилен между запусками.

- берётся из `MASKING_SALT` в `.env`; если там пусто — генерируется при первом старте
  (64 hex-символа, `masking/init_salt.py`). Хранится как Airflow Variable `masking_salt`
  в Postgres Airflow (volume `pg_data`), зашифрована Fernet-ключом `FERNET_KEY` из `.env`;
  `.env` в `.gitignore`, в git, SQL-файлах и `ml` соли нет;
- если Variable есть, но не расшифровывается текущим `FERNET_KEY`, контейнер Airflow
  не стартует и пишет `ОШИБКА: Variable masking_salt есть в базе Airflow, но не
  расшифровывается текущим FERNET_KEY…`. Новая соль в этом случае не создаётся;
- в ClickHouse передаётся параметром запроса `{salt:String}`;
- ClickHouse по умолчанию записывает в журнал `system.query_log` текст запроса
  с уже подставленными значениями параметров — соль оказывалась бы там в открытом
  виде. Поэтому для запросов с солью журнал отключён (`log_queries=0`, `masking/db.py`),
  а серверный лог понижен до `information`;
- Airflow заменяет значение на `***` в логах задач (`SENSITIVE_VAR_CONN_NAMES=salt`);
- `validate_ml` соль не использует.

Проверено: после запуска DAG соли нет в логах задач, логах контейнеров,
`system.query_log` и таблицах `ml`.

Стабильность псевдонима. Псевдоним не меняется, пока сохраняются volume `pg_data`
и `FERNET_KEY` в `.env`: перезапуск DAG, `docker compose restart`, `docker compose up -d
--force-recreate airflow` его не меняют (проверено по MD5 отсортированного списка
псевдонимов). Сменить соль (псевдонимы сменятся): `docker compose down -v` либо
новое значение `MASKING_SALT` в `.env`.

**Почему SHA-256, а не «простой» хэш.** Замер на стенде, 10 млн строк:
SHA-256 — 1,3 с, `sipHash64` — 0,33 с, `cityHash64` — 0,14 с. У нас ~20 тыс. строк,
то есть ~3 мс против ~0,3 мс — разница незаметна. Зато SHA-256
криптографический: по псевдонимам нельзя подобрать соль. Быстрые хэши
(`cityHash64`, `xxHash`) этого не гарантируют.

## k-анонимность

Квазиидентификаторы: `federal_district`, `rooms_range`, `connected_quarter`.
Размер группы считается по **клиентам** `dim_client` (окно `count() OVER (PARTITION BY …)`
в `dwh_client_qi.sql`), а не по строкам факта. Клиенты из групп меньше `K_MIN`
(`K_MIN` в `.env` → Variable `k_min`) в `ml` не попадают, их строки факта тоже.

При `K_MIN = 5` подавляется 195 клиентов из 1000 (3 883 строки факта), в `ml`
остаётся 805 клиентов в 65 группах, фактическое k = 5. Подавление в основном
касается малых округов и редких сочетаний «крупный отель + поздний квартал».

Эксперимент для оценки достаточности: `K_MIN` 3, 5, 10 и обобщение `connected_quarter`
до полугодия. Считается по `dwh` той же формулой КИ, `ml` и DAG не меняет:

```bash
docker compose exec -T airflow python -m masking.experiment md > experiments/k_min_experiment.md
```

```bash
docker compose exec -T airflow python -m masking.experiment csv > experiments/k_min_experiment.csv
```

| | квартал, 3 | квартал, 5 | квартал, 10 | полугодие, 3 | полугодие, 5 | полугодие, 10 |
|---|---|---|---|---|---|---|
| групп в ml | 96 | 65 | 36 | 66 | 55 | 34 |
| подавлено клиентов | 8,7 % | 19,5 % | 38,0 % | 2,6 % | 6,2 % | 20,0 % |
| подавлено строк факта | 8,2 % | 19,5 % | 37,5 % | 2,4 % | 5,8 % | 20,3 % |
| доля ушедших клиентов (в dwh 26,9 %) | 26,7 % | 27,7 % | 27,7 % | 26,8 % | 26,9 % | 27,4 % |

Распределение по округам и `rooms_range` — в `experiments/k_min_experiment.md`.

## Права в ClickHouse

| Пользователь | Права |
|---|---|
| `etl_user` | `ALL` на `dwh.*` и `ml.*` (Airflow) |
| `ml_user` | `SELECT` только на `ml.v_ml_client_month` и `ml.masking_log` |

Представление создано с `SQL SECURITY DEFINER` (определитель — `etl_user`): без этого
ClickHouse проверяет права вызывающего на `ml.fact_client_activity`, `ml.dim_client`
и т. д., и `ml_user` получает `ACCESS_DENIED` даже на представлении (проверено).
Права выдаются в `clickhouse/initdb/01_users.sh` при первом старте ClickHouse,
то есть применяются только к новому volume (`docker compose down -v`).

## Проверки `validate_ml`

Задача падает, если хотя бы одна проверка не прошла. `write_log` выполняется
всегда (`trigger_rule=all_done`) и пишет в `masking_log` статус `PASSED`/`FAILED`
с деталями.

| Проверка | Условие |
|---|---|
| `clients_balance` | клиентов в dwh = в ml + подавлено |
| `fact_balance` | строк факта в dwh = в ml + строки подавленных |
| `activity_sums` | суммы 8 показателей в ml = суммы по неподавленным клиентам dwh |
| `fact_orphans` | у строк факта ml есть пары в `dim_client`, `dim_tariff`, `dim_date` |
| `pseudonyms_unique` | нет коллизий псевдонимов |
| `no_source_keys` | исходных `client_key` нет ни в `dim_client`, ни в факте ml |
| `no_pii` | в ml нет исходных email, телефонов (в любом формате), ИНН, ФИО, названий отелей, PII-колонок; маски соответствуют шаблону |
| `k_anonymity` | фактическое k ≥ `K_MIN` |

Подавленные клиенты для балансов считаются в `dwh` той же формулой
(`dwh_client_qi.sql`), а не берутся из результата маскирования.

Негативный тест: вставка в `ml.dim_client` строки с исходным ключом и email —
`validate_ml` падает на `clients_balance`, `no_source_keys`, `no_pii`.

## Журнал `ml.masking_log`

Одна строка на запуск: `run_id` Airflow, время, строк прочитано/записано по
`dim_client`, `dim_tariff`, `dim_date`, `fact_client_activity`, подавлено клиентов
и строк факта, `K_MIN`, фактическое k, статус и JSON с результатами всех проверок.
Журнал не очищается при перезагрузке `ml`.
