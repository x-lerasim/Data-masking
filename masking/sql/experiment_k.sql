SELECT
    q.src_key AS src_key,
    q.federal_district AS federal_district,
    q.rooms_range AS rooms_range,
    q.connected_quarter AS connected_quarter,
    concat(toString(toYear(c.connected_date)), '-H', toString(intDiv(toQuarter(c.connected_date) + 1, 2)))
        AS connected_half,
    f.fact_rows AS fact_rows,
    f.churn_rows AS churn_rows
FROM (@QI_SOURCE@) AS q
INNER JOIN dwh.dim_client AS c ON c.client_key = q.src_key
LEFT JOIN
(
    SELECT client_key, count() AS fact_rows, sum(churn_flag) AS churn_rows
    FROM dwh.fact_client_activity
    GROUP BY client_key
) AS f ON f.client_key = q.src_key
ORDER BY src_key
