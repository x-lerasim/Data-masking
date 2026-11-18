{{ config(order_by='tariff_key') }}

SELECT tariff_key, tariff_name, price
FROM {{ source('dwh', 'dim_tariff') }}
