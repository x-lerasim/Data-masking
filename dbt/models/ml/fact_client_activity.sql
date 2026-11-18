{{ config(order_by='(client_key, date_key)') }}

SELECT
    {{ pseudo_key('client_key') }} AS client_key,
    tariff_key,
    date_key,
    logins_count,
    active_users,
    bookings_processed,
    features_used,
    tickets_count,
    late_payments,
    mrr,
    churn_flag
FROM {{ source('dwh', 'fact_client_activity') }}
