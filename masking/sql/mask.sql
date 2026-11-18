TRUNCATE TABLE ml.dim_client;
INSERT INTO ml.dim_client (client_key, city, rooms_count, connected_date)
SELECT @PSEUDO_KEY@, city, rooms_count, connected_date
FROM dwh.dim_client;

TRUNCATE TABLE ml.dim_tariff;
INSERT INTO ml.dim_tariff SELECT tariff_key, tariff_name, price FROM dwh.dim_tariff;

TRUNCATE TABLE ml.dim_date;
INSERT INTO ml.dim_date SELECT date_key, year, quarter, month FROM dwh.dim_date;

TRUNCATE TABLE ml.fact_client_activity;
INSERT INTO ml.fact_client_activity
    (client_key, tariff_key, date_key, logins_count, active_users, bookings_processed,
     features_used, tickets_count, late_payments, mrr, churn_flag)
SELECT
    @PSEUDO_KEY@, tariff_key, date_key, logins_count, active_users, bookings_processed,
    features_used, tickets_count, late_payments, mrr, churn_flag
FROM dwh.fact_client_activity;
