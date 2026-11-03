import os
import sys
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping

PROJECT = "/opt/airflow/project"

project_config = ProjectConfig(dbt_project_path=f"{PROJECT}/dbt_")
profile_config = ProfileConfig(
    profile_name="dbt_",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id='duckdb_conn',
        profile_args={
            "path": f"{PROJECT}",
            "extensions": ['httpfs', 'parquet'],
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
    env_vars={
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    },
)

@dag(
    dag_id='geckoapi_pipeline_dag',
    description='ETL Pipeline and Dimensional Modeling',
    start_date=datetime(2026, 3, 11),
    schedule="@daily",
    catchup=False,
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
    )

    extract_and_load >> transform >> dbt_build

geckoapi_pipeline_dag()