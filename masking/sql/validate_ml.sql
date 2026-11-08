-- Проверки слоя ml. Каждый блок начинается со строки "-- check: <имя> | <описание>"
-- и возвращает одну строку: ok (1/0), details (текст).
-- Соль не используется: исходные ключи ищутся сравнением значений, а не пересчётом хеша.

-- check: clients_balance | клиентов в dwh = клиентов в ml + подавлено
SELECT
    (SELECT count() FROM dwh.dim_client) AS dwh_n,
    (SELECT count() FROM ml.dim_client) AS ml_n,
    (SELECT count() FROM (@QI_SOURCE@) WHERE group_size < {k_min:UInt32}) AS suppressed,
    dwh_n = ml_n + suppressed AS ok,
    concat('dwh=', toString(dwh_n), ', ml=', toString(ml_n), ', подавлено=', toString(suppressed)) AS details;

-- check: fact_balance | строк факта в dwh = строк в ml + строки подавленных клиентов
SELECT
    (SELECT count() FROM dwh.fact_client_activity) AS dwh_n,
    (SELECT count() FROM ml.fact_client_activity) AS ml_n,
    (SELECT count() FROM dwh.fact_client_activity
      WHERE client_key IN (SELECT src_key FROM (@QI_SOURCE@) WHERE group_size < {k_min:UInt32})) AS suppressed,
    dwh_n = ml_n + suppressed AS ok,
    concat('dwh=', toString(dwh_n), ', ml=', toString(ml_n), ', подавлено=', toString(suppressed)) AS details;

-- check: activity_sums | суммы показателей в ml = суммы по неподавленным клиентам dwh
SELECT
    (SELECT tuple(sum(logins_count), sum(active_users), sum(bookings_processed), sum(features_used),
                  sum(tickets_count), sum(late_payments), sum(mrr), sum(churn_flag))
       FROM dwh.fact_client_activity
      WHERE client_key IN (SELECT src_key FROM (@QI_SOURCE@) WHERE group_size >= {k_min:UInt32})) AS dwh_s,
    (SELECT tuple(sum(logins_count), sum(active_users), sum(bookings_processed), sum(features_used),
                  sum(tickets_count), sum(late_payments), sum(mrr), sum(churn_flag))
       FROM ml.fact_client_activity) AS ml_s,
    dwh_s = ml_s AS ok,
    concat('logins, users, bookings, features, tickets, late, mrr, churn: dwh=', toString(dwh_s),
           ' ml=', toString(ml_s)) AS details;

-- check: fact_orphans | у каждой строки факта ml есть пара в dim_client, dim_tariff, dim_date
SELECT
    (SELECT count() FROM ml.fact_client_activity WHERE client_key NOT IN (SELECT client_key FROM ml.dim_client)) AS no_client,
    (SELECT count() FROM ml.fact_client_activity WHERE tariff_key NOT IN (SELECT tariff_key FROM ml.dim_tariff)) AS no_tariff,
    (SELECT count() FROM ml.fact_client_activity WHERE date_key   NOT IN (SELECT date_key   FROM ml.dim_date))   AS no_date,
    no_client + no_tariff + no_date = 0 AS ok,
    concat('без клиента=', toString(no_client), ', без тарифа=', toString(no_tariff),
           ', без даты=', toString(no_date)) AS details;

-- check: pseudonyms_unique | псевдонимы уникальны (нет коллизий)
SELECT
    (SELECT count() FROM ml.dim_client) AS n,
    (SELECT uniqExact(client_key) FROM ml.dim_client) AS u,
    n = u AS ok,
    concat('строк=', toString(n), ', уникальных псевдонимов=', toString(u)) AS details;

-- check: no_source_keys | в ml нет исходных client_key
SELECT
    (SELECT count() FROM ml.dim_client
      WHERE client_key IN (SELECT toString(client_key) FROM dwh.dim_client)) AS in_dim,
    (SELECT count() FROM ml.fact_client_activity
      WHERE client_key IN (SELECT toString(client_key) FROM dwh.dim_client)) AS in_fact,
    in_dim + in_fact = 0 AS ok,
    concat('исходных ключей: dim_client=', toString(in_dim), ', fact=', toString(in_fact)) AS details;

-- check: no_pii | в ml нет исходных email, телефонов, ИНН, ФИО, названий отелей и PII-колонок
WITH
    (SELECT groupArray(email) FROM dwh.dim_client) AS emails,
    (SELECT arrayConcat(groupArray(phone), groupArray(replaceRegexpAll(phone, '[^0-9]', ''))) FROM dwh.dim_client) AS phones,
    (SELECT groupArray(inn) FROM dwh.dim_client) AS inns,
    (SELECT arrayConcat(groupArray(contact_name), groupArray(hotel_name)) FROM dwh.dim_client) AS names
SELECT
    (SELECT countIf(has(emails, email_masked) OR has(phones, phone_masked)
                 OR arrayExists(v -> has(inns, v) OR has(phones, v) OR has(emails, v) OR has(names, v),
                                [client_key, email_masked, phone_masked])
                 OR NOT match(email_masked, '^[^@][*][*][*]@[^@]+$')
                 OR NOT match(phone_masked, '^[+]7 [*][*][*] [*][*][*] [0-9]{2} [0-9]{2}$'))
       FROM ml.dim_client) AS bad_values,
    (SELECT count() FROM system.columns
      WHERE database = 'ml'
        AND name IN ('contact_name', 'email', 'phone', 'inn', 'hotel_name', 'city', 'rooms_count', 'connected_date')) AS bad_columns,
    bad_values + bad_columns = 0 AS ok,
    concat('строк с PII или неверной маской=', toString(bad_values),
           ', PII-колонок в ml=', toString(bad_columns)) AS details;

-- check: k_anonymity | фактическое k в ml.dim_client >= K_MIN
SELECT
    (SELECT min(n) FROM (SELECT count() AS n FROM ml.dim_client
                          GROUP BY federal_district, rooms_range, connected_quarter)) AS k_actual,
    k_actual >= {k_min:UInt32} AS ok,
    concat('k=', toString(k_actual), ', K_MIN=', toString({k_min:UInt32})) AS details;
