FROM python:3.12-slim
RUN pip install --no-cache-dir dbt-clickhouse==1.10.3
WORKDIR /dbt
COPY . /dbt
ENV DBT_PROFILES_DIR=/dbt
CMD ["dbt", "--version"]
