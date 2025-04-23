import os
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# print(DEFAULT_DBT_ROOT_PATH,DBT_ROOT_PATH)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres",
        profile_args={"schema": "transformed"},
    )
)
with DAG(
    dag_id="dbt_transform",
    start_date=datetime(2025, 4, 22),
    schedule=None,
    catchup=False,
) as dag:

    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    dbt_task = DbtTaskGroup(
        group_id="dbt_task_group",
        project_config=ProjectConfig(
            (DBT_ROOT_PATH / "weather").as_posix(),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt"),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 2},
        dag=dag,
    )

    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow")

    pre_dbt_workflow >> dbt_task >> post_dbt_workflow