from __future__ import annotations

import os
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.client import Client

def get_client() -> Client:
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("ETL_USER", "etl_user"),
        password=os.environ["ETL_USER_PASSWORD"],
    )


def split_sql(text: str) -> list[str]:
    statements, buf = [], []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            buf = []
    tail = "\n".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def run_sql_file(client: Client, path: Path | str) -> None:
    for stmt in split_sql(Path(path).read_text(encoding="utf-8")):
        client.command(stmt)


def scalar(client: Client, sql: str):
    return client.query(sql).result_rows[0][0]
