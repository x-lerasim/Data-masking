-- Запросы для показа в http://localhost:8123/play
-- Play выполняет весь текст поля как один запрос: копируйте по одному запросу.
-- Пользователь и пароль вводятся в полях справа вверху страницы Play.

------------------------------------------------------------------------------
-- 1. Один и тот же клиент в dwh и в ml          (пользователь: etl_user)
------------------------------------------------------------------------------

-- 1.1 dwh: карточка клиента с персональными данными
SELECT client_key, contact_name, email, phone, inn, hotel_name, city, rooms_count, connected_date
FROM dwh.dim_client
WHERE client_key = 24;

-- 1.2 dwh: его активность по месяцам
SELECT f.date_key, t.tariff_name, f.logins_count, f.active_users, f.bookings_processed,
       f.features_used, f.tickets_count, f.late_payments, f.mrr, f.churn_flag
FROM dwh.fact_client_activity AS f
INNER JOIN dwh.dim_tariff AS t USING (tariff_key)
WHERE f.client_key = 24
ORDER BY f.date_key;

-- 1.3 ml: тот же клиент. Соли у аналитика нет, поэтому находим его
--     по совпадению помесячного ряда активности — те же цифры, но вместо ПДн
--     псевдоним, маски и обобщённые категории.
WITH
    (SELECT arraySort(groupArray((date_key, logins_count, bookings_processed, mrr)))
     FROM dwh.fact_client_activity WHERE client_key = 24) AS dwh_series
SELECT c.client_key AS pseudonym, c.email_masked, c.phone_masked,
       c.federal_district, c.rooms_range, c.connected_quarter,
       f.date_key, f.tariff_key, f.logins_count, f.active_users, f.bookings_processed,
       f.features_used, f.tickets_count, f.late_payments, f.mrr, f.churn_flag
FROM ml.fact_client_activity AS f
INNER JOIN ml.dim_client AS c ON f.client_key = c.client_key
WHERE f.client_key IN
(
    SELECT client_key FROM ml.fact_client_activity
    GROUP BY client_key
    HAVING arraySort(groupArray((date_key, logins_count, bookings_processed, mrr))) = dwh_series
)
ORDER BY f.date_key;

------------------------------------------------------------------------------
-- 2. Источник для модели оттока                  (пользователь: ml_user)
------------------------------------------------------------------------------

-- 2.1 витрина для ML: ни email_masked, ни phone_masked здесь нет
SELECT *
FROM ml.v_ml_client_month
ORDER BY client_key, date_key
LIMIT 20;

-- 2.2 доля оттока по округам — типичный признак для модели
SELECT federal_district,
       uniqExact(client_key) AS clients,
       sum(churn_flag) AS churned,
       round(churned / clients, 3) AS churn_rate
FROM ml.v_ml_client_month
GROUP BY federal_district
ORDER BY clients DESC;

------------------------------------------------------------------------------
-- 3. Фактическое k                               (пользователь: ml_user)
------------------------------------------------------------------------------

-- 3.1 минимальный размер группы по квазиидентификаторам (считается по клиентам)
SELECT min(clients) AS k_actual, count() AS groups, sum(clients) AS clients_total
FROM
(
    SELECT federal_district, rooms_range, connected_quarter, count() AS clients
    FROM ml.dim_client
    GROUP BY federal_district, rooms_range, connected_quarter
);

-- 3.2 самые маленькие группы
SELECT federal_district, rooms_range, connected_quarter, count() AS clients
FROM ml.dim_client
GROUP BY federal_district, rooms_range, connected_quarter
ORDER BY clients, federal_district
LIMIT 10;

-- 3.3 журнал запусков маскирования
SELECT run_id, logged_at, dim_client_read, dim_client_written, fact_read, fact_written,
       suppressed_clients, suppressed_fact_rows, k_min, k_actual, checks_status
FROM ml.masking_log
ORDER BY logged_at DESC;

------------------------------------------------------------------------------
-- 4. ml_user не видит dwh                        (пользователь: ml_user)
------------------------------------------------------------------------------

-- 4.1 ожидается ошибка ACCESS_DENIED (Code: 497)
SELECT * FROM dwh.dim_client LIMIT 5;
