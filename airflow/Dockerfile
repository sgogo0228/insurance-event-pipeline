FROM apache/airflow:3.0.6-python3.12

# dbt lives in its own virtualenv so its dependencies never conflict with Airflow's.
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-clickhouse==1.10.3

ENV PATH="${PATH}:/opt/airflow/dbt-venv/bin"
