{{ config(order_by='client_key') }}

SELECT
    {{ pseudo_key('client_key') }} AS client_key,
    city,
    rooms_count,
    connected_date
FROM {{ source('dwh', 'dim_client') }}
