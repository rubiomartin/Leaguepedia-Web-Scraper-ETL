from datetime import datetime
import os
import pandas as pd
from bs4 import BeautifulSoup
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.etl_functions import PostgresFileOperator, file_name
from pipelines.dag_pipeline import data_pipeline


default_args = {
    'owner': 'Martin Rubio',
    'start_date': datetime(2024, 6, 14)}


with DAG(
    dag_id='webscrapping_lolpedia',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    url = "https://lol.fandom.com/wiki/Special:RunQuery/TournamentStatistics?TS%5Bpreload%5D=TournamentByPlayer&TS%5Btournament%5D=Worlds+2018+Main+Event&TS%5Bshowstats%5D=2018&_run="
    league = 'LCK'
    year = '2020'
    tournament = 'Spring'

    table_name = file_name(league,year,tournament)

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=data_pipeline,
        op_kwargs={ 'url': url,
                    'league': league,
                    'year': year,
                    'tournament': tournament
                    },
        
    )
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql=f'''
       CREATE TABLE IF NOT EXISTS {table_name} (
        teams VARCHAR(255) DEFAULT NULL,
        player VARCHAR(255) DEFAULT NULL,
        games INT DEFAULT NULL,
        wins INT DEFAULT NULL,
        loses INT DEFAULT NULL,
        winrate_porcentage FLOAT DEFAULT NULL,
        k FLOAT DEFAULT NULL,
        d FLOAT DEFAULT NULL,
        a FLOAT DEFAULT NULL,        
        kda FLOAT DEFAULT NULL,
        cs FLOAT DEFAULT NULL,
        csm FLOAT DEFAULT NULL,
        g FLOAT DEFAULT NULL,
        gm FLOAT DEFAULT NULL,
        dmg FLOAT DEFAULT NULL,
        dmgm FLOAT DEFAULT NULL,
        kpar FLOAT DEFAULT NULL,
        ks FLOAT DEFAULT NULL,
        gs FLOAT DEFAULT NULL,
        champs_played INT DEFAULT NULL,
        champs_names VARCHAR(255) DEFAULT NULL
    );'''),
       

    insert_data = PostgresFileOperator(
        task_id = "insert_data",
        operation="write",
        config={"table_name":table_name},
    )


    extract >> create_table >> insert_data
