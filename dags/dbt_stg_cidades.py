from pendulum import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior


from airflow.models.baseoperator import chain

from pathlib import Path
import os



DBT_CONFIG = ProfileConfig(
    profile_name='novadrive',
    target_name='dev',
    profiles_yml_filepath=Path('/opt/airflow/dags/dbt/novadrive/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/opt/airflow/dags/dbt/novadrive',
)

DAG_ID = os.path.basename(__file__).replace(".py", "")


@dag(
    dag_id=DAG_ID,
    description=DAG_ID,
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    default_view='graph',
    tags=['dbt','auciello','test']
)
def dbt_stage():  

    facebook = DbtTaskGroup(
        group_id='stage',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['+stg_cidades'],
            test_behavior=TestBehavior.AFTER_EACH,
        )
    )

    # finish = EmptyOperator(task_id='finish')


    chain(
        facebook
    )

dbt_stage()