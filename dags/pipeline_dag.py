import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping
from dotenv import dotenv_values

PROJECT = "/opt/airflow/project"
env_file = dotenv_values(f"{PROJECT}/.env")

project_config = ProjectConfig(
    dbt_project_path=f"{PROJECT}/dbt_",
    env_vars={
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "TRANSFORM_DATETIME": env_file.get("TRANSFORM_DATETIME") or "1970-01-01T00-00-00",
    },
)
profile_config = ProfileConfig(
    profile_name="dbt_",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id='duckdb_conn',
        profile_args={
            "path": f"{PROJECT}/dev.duckdb",
            "extensions": ['httpfs', 'parquet'],
            "retries": {
                "connect_attempts": 5, 
                "retryable_exceptions": ["IOException"],
            },
            "secrets": [
                {
                    "type": "s3",
                    "region": "sa-east-1",
                    "key_id": "{{ env_var('AWS_ACCESS_KEY_ID') }}",
                    "secret": "{{ env_var('AWS_SECRET_ACCESS_KEY') }}",
                }
            ],
        },
    ),
)

@dag(
    dag_id='geckoapi_pipeline_dag',
    description='ETL Pipeline and Dimensional Modeling',
    start_date=datetime(2026, 3, 11),
    schedule="@daily",
    catchup=False,
    max_active_tasks=1,
)

def geckoapi_pipeline_dag():
    extract_and_load = BashOperator(
        task_id="extract_and_load_data",
        bash_command=f"cd {PROJECT} && python source/extract_and_load_data.py",
    )

    transform = BashOperator(
        task_id="transform_data",
        bash_command=f"cd {PROJECT} && python source/transform_data.py",
    )

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=project_config,
        profile_config=profile_config,
        operator_args={"retries": 5, "retry_delay": timedelta(seconds=60)}
    )

    extract_and_load >> transform >> dbt_build

geckoapi_pipeline_dag()