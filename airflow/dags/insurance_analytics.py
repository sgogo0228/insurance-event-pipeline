from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="insurance_analytics",
    description="Refresh the insurance analytics warehouse per business domain with dbt build",
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

    # Each domain is selected by its dbt tag; `dbt build` runs models and their tests in DAG order.
    build_policy = BashOperator(task_id="build_policy", bash_command="dbt build --select tag:policy")
    build_billing = BashOperator(task_id="build_billing", bash_command="dbt build --select tag:billing")

    # Cross-domain models (A & B & C -> D) only run when both domains succeeded.
    build_shared = BashOperator(task_id="build_shared", bash_command="dbt build --select tag:shared")

    source_freshness >> [build_policy, build_billing] >> build_shared
