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



with DAG(
    dag_id='pruebas',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    league = 'LCK'
    year = '2021'
    season = 'Spring'
    tournament = 'Season'

    table_name = f'{league}_{year}_{season}_{tournament}'

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=data_pipeline,
        op_kwargs={'league': league,
                    'year': year,
                    'season': season,
                    'tournament': tournament
                    },
        
    )
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql=f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            teams varchar(255),
            player varchar(255),
            games int NOT NULL,
            wins int NOT NULL,
            loses int NOT NULL,
            winrate varchar(255),
            k varchar(255),
            d varchar(255),
            a varchar(255),        
            kda varchar(255),
            cs varchar(255),
            csm varchar(255),
            g varchar(255),
            gm varchar(255),
            dmg varchar(255),
            dmgm varchar(255),
            kpar varchar(255),
            ks varchar(255),
            gs varchar(255),
            cp int NOT NULL,
            champs_names varchar(255)
        );
        ''',
       
    )

    insert_data = PostgresFileOperator(
        task_id = "insertar_data",
        operation="write",
        config={"table_name":table_name},
    )

    # creatingxd_csv = PythonOperator(
    #     task_id='create_csv',
    #     python_callable=creating_csv,
    #     dag=dag
    # )


    extract  >>create_table >> insert_data