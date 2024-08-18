from datetime import datetime
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json
from airflow.models import Variable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.etl_functions import data_pipeline, PostgresFileOperator


default_args = {
    'owner': 'Martin Rubio',
    'start_date': datetime(2024, 6, 14)
}



dag = DAG(
    dag_id='lolstatslck',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=data_pipeline,
    op_kwargs={
        'league': 'LCK',
        'year': '2023',
        'season': 'Spring',
        'tournament': 'Season'
        },
    dag=dag
)
# create_table = PostgresOperator(
#     task_id='create_table',
#     postgres_conn_id='postgres_default',
#     sql='''
#     CREATE TABLE IF NOT EXISTS statslol (
#         teams text DEFAULT 'unknown',
#         player text DEFAULT 'no_title',
#         games text DEFAULT 'unknown',
#         wins text DEFAULT 'unknown',
#         loses text DEFAULT 'unknown',
#         winrate text DEFAULT 'unknown',
#         k text DEFAULT 'unknown',
#         d text DEFAULT 'unknown',
#         a text DEFAULT 'unknown',        
#         kda text DEFAULT 'unknown',
#         cs text DEFAULT 'unknown',
#         csm text DEFAULT 'unknown',
#         g text DEFAULT 'unknown',
#         gm text DEFAULT 'unknown',
#         dmg text DEFAULT 'unknown',
#         dmgm text DEFAULT 'unknown',
#         kpar text DEFAULT 'unknown',
#         ks text DEFAULT 'unknown',
#         gs text DEFAULT 'unknown',
#         cp int DEFAULT 0,
#         champs_names text DEFAULT 'unknown'
#     );
#     ''',
#     dag=dag
# )

insert_data = PostgresFileOperator(
    task_id = "insertar_data",
    operation="write",
    config={"table_name":"statslol"},
    dag=dag
)

# creatingxd_csv = PythonOperator(
#     task_id='create_csv',
#     python_callable=creating_csv,
#     dag=dag
# )


extract  >> insert_data