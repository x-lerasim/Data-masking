-- dwh.fact_client_activity → ml.fact_client_activity.
-- Показатели без изменений, client_key → тот же псевдоним, что в dim_client.
-- Строки подавленных клиентов не переносятся.
TRUNCATE TABLE ml.fact_client_activity;

INSERT INTO ml.fact_client_activity
    (client_key, tariff_key, date_key, logins_count, active_users, bookings_processed,
     features_used, tickets_count, late_payments, mrr, churn_flag)
SELECT
    @PSEUDO_KEY@ AS pseudo_key,
    tariff_key, date_key, logins_count, active_users, bookings_processed,
    features_used, tickets_count, late_payments, mrr, churn_flag
FROM dwh.fact_client_activity
WHERE client_key IN (SELECT src_key FROM (@QI_SOURCE@) WHERE group_size >= {k_min:UInt32});
