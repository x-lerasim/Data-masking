"""Модуль маскирования dwh → ml.

Вся трансформация — в SQL-файлах masking/sql/, правила по полям — в rules.yaml.
Здесь только сборка параметров, запуск SQL и подсчёт статистики.

Соль передаётся в ClickHouse только как параметр запроса {salt:String}
(server-side binding clickhouse-connect): в тексте SQL её нет, в ml она не пишется,
функции модуля её не логируют и не возвращают.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

import yaml

from masking.db import SQL_DIR, get_client, run_sql, run_sql_file, scalar

RULES_PATH = Path(__file__).parent / "rules.yaml"
PSEUDO_KEY = "lower(hex(substring(SHA256(concat({salt:String}, ':', toString(client_key))), 1, 8)))"
MIN_SALT_LEN = 16


@lru_cache
def rules() -> dict:
    return yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))


def _render(sql_name: str) -> str:
    """Подставляет общие фрагменты (@QI_SOURCE@, @PSEUDO_KEY@) в SQL-файл."""
    text = (SQL_DIR / sql_name).read_text(encoding="utf-8")
    qi = (SQL_DIR / "dwh_client_qi.sql").read_text(encoding="utf-8")
    return text.replace("@QI_SOURCE@", "\n" + qi + "\n").replace("@PSEUDO_KEY@", PSEUDO_KEY)


def _rule_params(k_min: int) -> dict:
    r = rules()
    mapping = r["city_to_district"]
    return {
        "cities": list(mapping.keys()),
        "districts": list(mapping.values()),
        "unknown_district": r["unknown_district"],
        "room_edges": r["rooms_ranges"]["edges"],
        "room_labels": r["rooms_ranges"]["labels"],
        "k_min": int(k_min),
    }


def check_salt(salt: str | None) -> None:
    if not salt or len(salt) < MIN_SALT_LEN:
        raise ValueError(f"Соль не задана или короче {MIN_SALT_LEN} символов (Variable masking_salt / .env MASKING_SALT)")


def check_rules_cover_schema(ch) -> None:
    """Каждая колонка dwh-таблицы должна иметь правило в rules.yaml."""
    tables = rules()["tables"]
    for table, fields in tables.items():
        cols = [r[0] for r in ch.query(
            "SELECT name FROM system.columns WHERE database = 'dwh' AND table = {t:String}",
            parameters={"t": table}).result_rows]
        if not cols:
            raise RuntimeError(f"dwh.{table} не найдена")
        missing = [c for c in cols if c not in fields and "*" not in fields]
        if missing:
            raise RuntimeError(f"dwh.{table}: нет правил маскирования для колонок {missing}")


def _counts(ch, table: str) -> int:
    return scalar(ch, f"SELECT count() FROM {table}")


def mask_dim_client(salt: str, k_min: int) -> dict:
    check_salt(salt)
    ch = get_client()
    check_rules_cover_schema(ch)
    run_sql_file(ch, SQL_DIR / "ddl_ml.sql")
    params = _rule_params(k_min) | {"salt": salt}
    run_sql(ch, _render("mask_dim_client.sql"), params)
    read, written = _counts(ch, "dwh.dim_client"), _counts(ch, "ml.dim_client")
    unknown = scalar(ch, "SELECT count() FROM ml.dim_client WHERE federal_district = {u:String}",
                     {"u": rules()["unknown_district"]})
    k_actual = scalar(ch, """SELECT min(n) FROM (SELECT count() AS n FROM ml.dim_client
                             GROUP BY federal_district, rooms_range, connected_quarter)""")
    groups_total = scalar(ch, "SELECT uniqExact(federal_district, rooms_range, connected_quarter) "
                              "FROM (" + (SQL_DIR / "dwh_client_qi.sql").read_text(encoding="utf-8") + ")",
                          _rule_params(k_min))
    return {"dim_client_read": read, "dim_client_written": written,
            "suppressed_clients": read - written, "k_min": int(k_min), "k_actual": int(k_actual or 0),
            "qi_groups_total": groups_total, "unknown_city_clients": unknown}


def copy_dims() -> dict:
    ch = get_client()
    run_sql_file(ch, SQL_DIR / "copy_dims.sql")
    return {"dim_tariff_read": _counts(ch, "dwh.dim_tariff"), "dim_tariff_written": _counts(ch, "ml.dim_tariff"),
            "dim_date_read": _counts(ch, "dwh.dim_date"), "dim_date_written": _counts(ch, "ml.dim_date")}


def mask_fact(salt: str, k_min: int) -> dict:
    check_salt(salt)
    ch = get_client()
    run_sql(ch, _render("mask_fact.sql"), _rule_params(k_min) | {"salt": salt})
    read, written = _counts(ch, "dwh.fact_client_activity"), _counts(ch, "ml.fact_client_activity")
    return {"fact_read": read, "fact_written": written, "suppressed_fact_rows": read - written}


def _parse_checks(text: str) -> list[tuple[str, str, str]]:
    """Разбирает validate_ml.sql на блоки (имя, описание, SQL)."""
    blocks = re.split(r"^-- check:\s*", text, flags=re.M)[1:]
    out = []
    for b in blocks:
        header, _, body = b.partition("\n")
        name, _, desc = header.partition("|")
        sql = "\n".join(l for l in body.splitlines() if not l.strip().startswith("--")).strip().rstrip(";")
        out.append((name.strip(), desc.strip(), sql))
    return out


def validate(k_min: int) -> list[dict]:
    """Выполняет все проверки и возвращает их результат (без исключения)."""
    ch = get_client()
    params = _rule_params(k_min)
    results = []
    for name, desc, sql in _parse_checks(_render("validate_ml.sql")):
        res = ch.query(sql, parameters=params)
        row = dict(zip(res.column_names, res.result_rows[0]))
        results.append({"check": name, "description": desc, "ok": bool(row["ok"]), "details": row["details"]})
    return results


def write_log(run_id: str, stats: dict, checks: list[dict] | None) -> str:
    ch = get_client()
    run_sql_file(ch, SQL_DIR / "ddl_ml.sql")
    if checks is None:
        status, details = "FAILED", "проверки не выполнены (упала предыдущая задача)"
    else:
        failed = [c["check"] for c in checks if not c["ok"]]
        status = "FAILED" if failed else "PASSED"
        details = json.dumps(checks, ensure_ascii=False)
    cols = ["run_id", "dim_client_read", "dim_client_written", "dim_tariff_read", "dim_tariff_written",
            "dim_date_read", "dim_date_written", "fact_read", "fact_written", "suppressed_clients",
            "suppressed_fact_rows", "k_min", "k_actual", "checks_status", "checks_details"]
    row = [run_id] + [int(stats.get(c, 0) or 0) for c in cols[1:13]] + [status, details]
    ch.insert("ml.masking_log", [row], column_names=cols)
    return status
