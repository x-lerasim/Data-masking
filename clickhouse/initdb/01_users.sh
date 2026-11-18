#!/bin/bash
set -euo pipefail
clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS dwh COMMENT 'Слой 1: витрина с персональными данными';
CREATE DATABASE IF NOT EXISTS ml  COMMENT 'Слой 2: обезличенная копия для ML';

CREATE USER IF NOT EXISTS etl_user IDENTIFIED WITH sha256_password BY '${ETL_USER_PASSWORD}';
GRANT ALL ON dwh.* TO etl_user;
GRANT ALL ON ml.*  TO etl_user;

CREATE USER IF NOT EXISTS ml_user IDENTIFIED WITH sha256_password BY '${ML_USER_PASSWORD}';
GRANT SELECT ON ml.* TO ml_user;
SQL
