from __future__ import annotations

import pendulum
from airflow.sdk import Variable, dag, get_current_context, task
from airflow.utils.trigger_rule import TriggerRule

from masking import masker
from synthetic.generate import generate


def _salt() -> str:
    salt = Variable.get("masking_salt")
    masker.check_salt(salt)
    return salt


def _k_min() -> int:
    return int(Variable.get("k_min", default=5))


@dag(
    dag_id="mask_dwh_to_ml",
    description="Маскирование ПДн: dwh (звезда с ПДн) → ml (обезличенная звезда для модели оттока)",
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
    def mask_dim_client() -> dict:
        stats = masker.mask_dim_client(_salt(), _k_min())
        print(f"dim_client: {stats}")
        return stats

    @task
    def copy_dims() -> dict:
        stats = masker.copy_dims()
        print(f"dim_tariff, dim_date: {stats}")
        return stats

    @task
    def mask_fact() -> dict:
        stats = masker.mask_fact(_salt(), _k_min())
        print(f"fact_client_activity: {stats}")
        return stats

    @task
    def validate_ml() -> None:
        checks = masker.validate(_k_min())
        get_current_context()["ti"].xcom_push(key="checks", value=checks)
        for c in checks:
            print(f"[{'OK  ' if c['ok'] else 'FAIL'}] {c['check']}: {c['description']} — {c['details']}")
        failed = [c["check"] for c in checks if not c["ok"]]
        if failed:
            raise ValueError(f"Проверки ml не пройдены: {failed}")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def write_log() -> str:
        ctx = get_current_context()
        ti = ctx["ti"]
        stats: dict = {}
        for t in ("mask_dim_client", "copy_dims", "mask_fact"):
            stats |= ti.xcom_pull(task_ids=t) or {}
        checks = ti.xcom_pull(task_ids="validate_ml", key="checks")
        status = masker.write_log(ctx["run_id"], stats, checks)
        print(f"masking_log: run_id={ctx['run_id']} status={status}")
        if status != "PASSED":
            raise ValueError("Запуск записан в журнал со статусом FAILED")
        return status

    generate_dwh() >> mask_dim_client() >> copy_dims() >> mask_fact() >> validate_ml() >> write_log()


mask_dwh_to_ml()
