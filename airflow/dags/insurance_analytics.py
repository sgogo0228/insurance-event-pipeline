from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="insurance_analytics",
    description="Refresh the insurance analytics warehouse layer by layer with dbt build",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Never let two runs overlap: the next run waits until the previous one has finished.
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["dbt", "clickhouse"],
) as dag:
    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command="dbt source freshness",
    )

    # `dbt build` runs models and their tests in DAG order; a failing test skips everything downstream.
    build_staging = BashOperator(task_id="build_staging", bash_command="dbt build --select staging")
    build_intermediate = BashOperator(task_id="build_intermediate", bash_command="dbt build --select intermediate")
    build_marts = BashOperator(task_id="build_marts", bash_command="dbt build --select marts")

    source_freshness >> build_staging >> build_intermediate >> build_marts
