"""Подключение к ClickHouse (clickhouse-connect) и выполнение SQL-файлов."""
from __future__ import annotations

import os
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.client import Client

SQL_DIR = Path(__file__).parent / "sql"


def get_client() -> Client:
    """Клиент под etl_user: единственный пользователь с доступом к dwh и ml."""
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("ETL_USER", "etl_user"),
        password=os.environ["ETL_USER_PASSWORD"],
    )


def split_sql(text: str) -> list[str]:
    """Делит файл на отдельные запросы по ';' в конце строки, убирает комментарии-строки."""
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


# ClickHouse пишет в system.query_log текст запроса с уже подставленными
# значениями параметров. Для запросов с секретом журнал запросов отключаем.
SECRET_PARAMS = {"salt"}
NO_QUERY_LOG = {"log_queries": 0, "log_query_threads": 0, "log_query_views": 0}


def run_sql(client: Client, text: str, parameters: dict | None = None) -> None:
    """Выполняет все запросы текста. Параметры передаются серверу как {name:Type}
    (server-side binding), в текст SQL не подставляются."""
    settings = NO_QUERY_LOG if SECRET_PARAMS & set(parameters or {}) else None
    for stmt in split_sql(text):
        client.command(stmt, parameters=parameters or None, settings=settings)


def run_sql_file(client: Client, path: Path | str, parameters: dict | None = None) -> None:
    run_sql(client, Path(path).read_text(encoding="utf-8"), parameters)


def scalar(client: Client, sql: str, parameters: dict | None = None):
    return client.query(sql, parameters=parameters or None).result_rows[0][0]
