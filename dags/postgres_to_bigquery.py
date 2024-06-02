'''
# Carga incremental do Postgres para Bigquery
- Leitura em cada uma das tabelas do Bigquery para identificar o maior ID já preenchido
- A partir deste Id Máximo é feita uma leitura do Postgres
- Os dados são salvos em um Dataframe
- O Dataframe e Anexado na tabela do Bigquery 
'''


from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.empty import EmptyOperator

import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
import decimal

 
default_args = {
    'owner': 'daniel',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}
 
@dag(
    dag_id='postgres_to_bigquery',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule_interval=timedelta(days=1),
    catchup=False,
    doc_md=__doc__
)
def postgres_to_bigquery_etl():
    
    project_id = 'auciello-design'
    dataset = 'novadrive'
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']
    CREDENTIALS = service_account.Credentials.from_service_account_file('/opt/airflow/config/auciello-design.json')

    postgres_source_task = EmptyOperator(task_id="postgres_source")
    bigquery_raw_task = EmptyOperator(task_id="bigquery_raw")

    for table_name in table_names:
        @task(task_id=f'get_current_max_id_{table_name}_from_bigquery')
        def get_max_primary_key(table_name: str):

            query = f'''
                SELECT COALESCE(MAX(ID_{table_name}),0) as max_id 
                FROM {project_id}.{dataset}.{table_name}
            '''
            results = pandas_gbq.read_gbq(
                query,
                project_id=project_id,
                credentials = CREDENTIALS,
                dialect='standard'
            )
            max_id = results['max_id'].iloc[0]
            print(max_id)
            
            return max_id

        @task(task_id=f'load_data_{table_name}_from_postgres_to_bigquery')
        def load_incremental_data(table_name: str, max_id: int):
            with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    primary_key = f'ID_{table_name}'
                    
                    pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                    columns = [row[0] for row in pg_cursor.fetchall()]
                    columns_list_str = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))
                    
                    pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > {max_id}")
                    rows = pg_cursor.fetchall()
                    if len(rows) > 0:
                        print(rows)
                        # Criação de um dicionário com o Schema do Postgres
                        dict_pg_schema = {}
                        for i, row in enumerate(rows):
                            for j, valor in enumerate(row):
                                if i == 0:
                                    dict_pg_schema[columns[j]] = str(type(valor))
                        print(dict_pg_schema)
                        # Criação de um Dataframe baseado no resultado do Postgres
                        df_pg = pd.DataFrame(rows, columns=columns)

                        for col in columns:
                            print(dict_pg_schema[col])
                            print(type(dict_pg_schema[col]))
                            # Conversão de tipos de colunas do Postgres -> DataFrame -> Bigquery
                            if dict_pg_schema[col] == "<class 'decimal.Decimal'>":
                                df_pg[col] = df_pg[col].astype(float)

                        pandas_gbq.to_gbq(df_pg, 
                            destination_table=f"{dataset}.{table_name}", 
                            project_id=project_id, 
                            if_exists='append', 
                            credentials=CREDENTIALS)
                        

        max_id = get_max_primary_key(table_name)
        postgres_source_task.set_downstream(max_id)
        load = load_incremental_data(table_name, max_id)
        bigquery_raw_task.set_upstream(load)
 
postgres_to_bigquery_etl_dag = postgres_to_bigquery_etl()


