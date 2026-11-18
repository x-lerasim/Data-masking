from __future__ import annotations

import pendulum
from airflow.sdk import Variable, dag, task

from masking import masker
from synthetic.generate import generate


@dag(
    dag_id="mask_dwh_to_ml",
    description="Обезличивание: dwh (звезда с ПДн) → ml (без ПДн, client_key — хеш с солью)",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["masking", "clickhouse", "demo"],
)
def mask_dwh_to_ml():
    @task
    def generate_dwh() -> dict:
        stats = generate()
        print(f"dwh заполнен: {stats}")
        return stats

    @task
    def mask_to_ml() -> dict:
        counts = masker.mask_to_ml(Variable.get("masking_salt"))
        for table, n in counts.items():
            print(f"{table}: {n}")
        return counts

    generate_dwh() >> mask_to_ml()


mask_dwh_to_ml()
