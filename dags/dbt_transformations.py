'''
# Transformações de Dados no DBT
- As fontes de dados foram criadas na etapa anterior de Extração e serão as sources do DBT
- Após isso será criada a camada stage e então fact e dimension
- A camada final de analyses representa os dados já transformados e tratados para serem expostos em uma ferramenta de visualização
'''
from pendulum import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior


from cosmos import ExecutionConfig, ExecutionMode

from airflow.models.baseoperator import chain

from pathlib import Path
import os

EXECUTION_CONFIG = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

DBT_CONFIG = ProfileConfig(
    profile_name='novadrive',
    target_name='dev',
    profiles_yml_filepath=Path('/opt/airflow/dags/dbt/novadrive/profiles.yml')
)


DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/opt/airflow/dags/dbt/novadrive',
    # manifest_path='/opt/airflow/dags/dbt/novadrive/target/manifest.json',
)

DAG_ID = os.path.basename(__file__).replace(".py", "")


@dag(
    dag_id=DAG_ID,
    description=DAG_ID,
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    concurrency=5,
    default_view='graph',
    tags=['dbt','novadrive'],
    doc_md=__doc__
)
def dbt_transformations():  

    dbt_stage_task = DbtTaskGroup(
        group_id='stage',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_deps=False,
            select=['stage'],
            test_behavior=TestBehavior.AFTER_EACH,
        )
    )

    dbt_dim_task = DbtTaskGroup(
        group_id='dim',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_deps=False,
            select=['dim'],
            test_behavior=TestBehavior.AFTER_EACH,
        )
    )

    # finish = EmptyOperator(task_id='finish')


    chain(
        dbt_stage_task,
        dbt_dim_task
    )

dbt_transformations()