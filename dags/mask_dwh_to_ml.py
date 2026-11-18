from __future__ import annotations

import os

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task

from synthetic.generate import generate


@dag(
    dag_id="mask_dwh_to_ml",
    description="Обезличивание: dwh (звезда с ПДн) → ml (dbt: без ПДн, client_key — хеш с солью)",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["masking", "clickhouse", "dbt", "demo"],
)
def mask_dwh_to_ml():
    @task
    def generate_dwh() -> dict:
        stats = generate()
        print(f"dwh заполнен: {stats}")
        return stats

    mask_to_ml = DockerOperator(
        task_id="mask_to_ml",
        image="data-masking-dbt:latest",
        command="dbt --no-use-colors run",
        docker_url="unix://var/run/docker.sock",
        network_mode="data-masking_default",
        auto_remove="success",
        mount_tmp_dir=False,
        environment={"CLICKHOUSE_HOST": "clickhouse", "ETL_USER": "etl_user"},
        private_environment={
            "ETL_USER_PASSWORD": os.environ.get("ETL_USER_PASSWORD", ""),
            "MASKING_SALT": os.environ.get("MASKING_SALT", ""),
        },
    )

    generate_dwh() >> mask_to_ml


mask_dwh_to_ml()
