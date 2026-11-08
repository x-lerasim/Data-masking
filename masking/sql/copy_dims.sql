-- Измерения без персональных данных копируются без изменений.
TRUNCATE TABLE ml.dim_tariff;
INSERT INTO ml.dim_tariff SELECT tariff_key, tariff_name, price FROM dwh.dim_tariff;

TRUNCATE TABLE ml.dim_date;
INSERT INTO ml.dim_date SELECT date_key, year, quarter, month FROM dwh.dim_date;
