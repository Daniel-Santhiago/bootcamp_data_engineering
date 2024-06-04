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

from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup

from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior, DbtResourceType
from cosmos.dbt.graph import DbtNode
from cosmos.operators.local import DbtRunLocalOperator, DbtSnapshotLocalOperator

from cosmos import ExecutionConfig, ExecutionMode

from airflow.models.baseoperator import chain

from pathlib import Path
import os

EXECUTION_CONFIG = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

DBT_PROJECT_DIR = '/opt/airflow/dags/dbt/novadrive'

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

class DbtSourceColoredOperator(EmptyOperator):
    ui_color = "#5FB825"
    ui_fgcolor = "white"

class DbtModelColoredOperator(DbtRunLocalOperator):
    ui_color = "#0094B3"
    ui_fgcolor = "#FFFFFF"

class DbtSnapshotColoredOperator(DbtSnapshotLocalOperator):
    ui_color = "#88447D"
    ui_fgcolor = "#FFFFFF"


def convert_source(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "source" node.
    """
    return DbtSourceColoredOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_source")


# Cosmos will use this function to generate an empty task when it finds a exposure node, in the manifest.
def convert_exposure(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "exposure" node.
    """
    return EmptyOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_exposure")


def convert_model(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "model" node.
    """
    return DbtModelColoredOperator(dag=dag, task_group=task_group, task_id=f"{node.name}", 
                                   profile_config=DBT_CONFIG, project_dir=DBT_PROJECT_DIR)


def convert_snapshot(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "snapshot" node.
    """
    return DbtSnapshotColoredOperator(dag=dag, task_group=task_group, task_id=f"{node.name}", 
                                   profile_config=DBT_CONFIG, project_dir=DBT_PROJECT_DIR)




@dag(
    dag_id=DAG_ID,
    description=DAG_ID,
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    concurrency=7,
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
            node_converters={
                DbtResourceType("source"): convert_source,  # known dbt node type to Cosmos (part of DbtResourceType)
                DbtResourceType("exposure"): convert_exposure,  # dbt node type new to Cosmos (will be added to DbtResourceType)
                DbtResourceType("model"): convert_model,  
                DbtResourceType("snapshot"): convert_snapshot,
            }
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
            select=['dimensions'],
            test_behavior=TestBehavior.AFTER_EACH,
            node_converters={
                DbtResourceType("source"): convert_source,  # known dbt node type to Cosmos (part of DbtResourceType)
                DbtResourceType("exposure"): convert_exposure,  # dbt node type new to Cosmos (will be added to DbtResourceType)
                DbtResourceType("model"): convert_model,  
                DbtResourceType("snapshot"): convert_snapshot,
            }
        )
    )

    # finish = EmptyOperator(task_id='finish')


    chain(
        dbt_stage_task,
        dbt_dim_task
    )

dbt_transformations()