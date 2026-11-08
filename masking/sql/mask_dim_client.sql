-- dwh.dim_client → ml.dim_client.
-- ФИО, название отеля и ИНН не выбираются. Клиенты из групп < k_min подавляются.
-- @PSEUDO_KEY@ — выражение псевдонима; соль приходит параметром {salt:String}.
TRUNCATE TABLE ml.dim_client;

INSERT INTO ml.dim_client
    (client_key, email_masked, phone_masked, federal_district, rooms_range, connected_quarter)
SELECT
    @PSEUDO_KEY@ AS pseudo_key,
    concat(leftUTF8(c.email, 1), '***', substring(c.email, position(c.email, '@'))) AS email_masked,
    concat('+7 *** *** ', substring(digits, 8, 2), ' ', substring(digits, 10, 2)) AS phone_masked,
    q.federal_district,
    q.rooms_range,
    q.connected_quarter
FROM
(
    SELECT client_key, email, replaceRegexpAll(phone, '[^0-9]', '') AS digits
    FROM dwh.dim_client
) AS c
INNER JOIN (@QI_SOURCE@) AS q ON c.client_key = q.src_key
WHERE q.group_size >= {k_min:UInt32};
