from __future__ import annotations

import pendulum

from airflow.models.dag import DAG

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig


# Absolute paths where the DBT project should be located inside Google Cloud Composer
DBT_PROJECT_PATH = "/home/airflow/gcs/dags/dbt"
PROFILES_FILE_PATH = "/home/airflow/gcs/dags/dbt/profiles.yml"

# Set Cosmos connection to BigQuery
# The 'ancient_gaming' profile should be referenced in the dbt profiles.yml file
profile_config = ProfileConfig(
    profile_name="ancient_gaming",
    target_name="dev",
    profiles_yml_filepath=PROFILES_FILE_PATH,
)

# Define the Airflow DAG
with DAG(
    dag_id="dbt_ancient_gaming_pipeline",
    start_date=pendulum.datetime(2025, 9, 15, tz="UTC"),
    schedule="@daily", # This fulfills the daily refresh requirement
    catchup=False,
    tags=["dbt", "bigquery", "ancient_gaming"],
    doc_md="""
    ### Ancient Gaming DBT Pipeline
    Orchestrates the daily execution of the Ancient Gaming DBT models on BigQuery.
    """,
) as dag:
    
    # This DbtTaskGroup will automatically parse your dbt project and create
    # an Airflow task for each model and test, respecting all dependencies.
    dbt_task_group = DbtTaskGroup(
        group_id="dbt_transformation_tasks",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args={
            # This ensures dbt dependencies are installed before the run
            "install_deps": True,
        },
    )