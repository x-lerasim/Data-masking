-- Квазиидентификаторы клиентов dwh и размер их группы.
-- Подставляется подзапросом вместо @QI_SOURCE@ (маскирование и проверки
-- считают подавление одной и той же формулой). Соль здесь не нужна.
SELECT
    client_key AS src_key,
    transform(city, {cities:Array(String)}, {districts:Array(String)}, {unknown_district:String})
        AS federal_district,
    {room_labels:Array(String)}[arrayCount(e -> rooms_count > e, {room_edges:Array(UInt16)}) + 1]
        AS rooms_range,
    concat(toString(toYear(connected_date)), '-Q', toString(toQuarter(connected_date)))
        AS connected_quarter,
    count() OVER (PARTITION BY federal_district, rooms_range, connected_quarter) AS group_size
FROM dwh.dim_client
