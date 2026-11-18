{{ config(order_by='date_key') }}

SELECT date_key, year, quarter, month
FROM {{ source('dwh', 'dim_date') }}
