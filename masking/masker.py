from __future__ import annotations

from masking.db import SQL_DIR, get_client, run_sql, run_sql_file, scalar

PSEUDO_KEY = "lower(hex(substring(SHA256(concat({salt:String}, ':', toString(client_key))), 1, 8)))"
MIN_SALT_LEN = 16
TABLES = ("dim_client", "dim_tariff", "dim_date", "fact_client_activity")


def check_salt(salt: str | None) -> None:
    if not salt or len(salt) < MIN_SALT_LEN:
        raise ValueError(f"Соль не задана или короче {MIN_SALT_LEN} символов (Variable masking_salt / .env MASKING_SALT)")


def mask_to_ml(salt: str) -> dict:
    check_salt(salt)
    ch = get_client()
    run_sql_file(ch, SQL_DIR / "ddl_ml.sql")
    sql = (SQL_DIR / "mask.sql").read_text(encoding="utf-8").replace("@PSEUDO_KEY@", PSEUDO_KEY)
    run_sql(ch, sql, {"salt": salt})
    return {f"{db}.{t}": scalar(ch, f"SELECT count() FROM {db}.{t}") for t in TABLES for db in ("dwh", "ml")}
