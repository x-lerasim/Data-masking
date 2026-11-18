import os

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="mask_dwh_to_ml",
    description="Обезличивание: dwh (звезда с ПДн) → ml (dbt: без ПДн, client_key — хеш с солью)",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["masking", "clickhouse", "dbt", "demo"],
):
    generate_dwh = BashOperator(
        task_id="generate_dwh",
        bash_command="python /opt/airflow/project/synthetic/generate.py",
    )

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

    generate_dwh >> mask_to_ml
