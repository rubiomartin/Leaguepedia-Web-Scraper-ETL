import pandas as pd
import requests
from bs4 import BeautifulSoup
import sys

from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def extract_data (league: str, year:str, season: str, tournament:str):
    try:
        url = f"https://lol.fandom.com/wiki/Special:RunQuery/TournamentStatistics?TS%5Btournament%5D={league}%2F{year}+Season%2F{season}+{tournament}&TS%5Bpreload%5D=TournamentByPlayer&_run="
        print(url)
        response = requests.get(url)
        if response.status_code == 200:
       
            html_content = response.content

      
            soup = BeautifulSoup(html_content, "html.parser")

            tabla = soup.find_all('tbody')[0]
            return tabla
    except Exception as e:
        print(e)
        sys.exit(1)


def create_dataframe (tabla):
    columnas = ['player', 'games','wins','loses','winrate','k','d','a','kda','cs','csm','g','gm','dmg','dmgm','kpar','ks','gs','cp']
    df = pd.DataFrame(columns = columnas)
    img_element = tabla.find_all('img')
    
    descripciones = []
    
    for desc in img_element:
        descripciones.append(desc.get('alt'))
    for indice, cadena in enumerate(descripciones):
        if "logo std" in cadena:
            descripciones[indice] = cadena.replace("logo std", "")

    tr = tabla.find_all('tr')
    for j in range (5, len(tr)):
        row_data = tr[j].find_all('td')
        individual_row_data = [row_data[i].text for i in range (1, len(row_data)-1)]
        lenght = len(df)
        df.loc[lenght] = individual_row_data


    span = [s for s in tabla.find_all('span') if 'title' in s.attrs]
    champs = ""
    champs_names= []
    i = 0
    df['cp'] = df['cp'].astype(int)
    for index, row in df.iterrows():
        cp = row['cp']

        if cp >= 3:
            champs = span[i]['title'] + ", " + span[i+1]['title'] + ", " + span[i+2]['title']
            champs_names.append(champs)
            i += 3
        elif cp == 2:
            champs = span[i]['title'] + ", " + span[i+1]['title']
            champs_names.append(champs)
            i += 2
        else:
            champs = span[i]['title']
            champs_names.append(champs)
            i += 1

    df['champs_names'] = champs_names
    df.insert(0, 'teams', descripciones)

    return df


def clean_dataframe (df):
    df['winrate'] = df['winrate'].apply(lambda x: float(x.replace('%', '')) if '%' in x else x)
    df['kpar'] = df['kpar'].apply(lambda x: float(x.replace('%', '')) if '%' in x else x)
    df['ks'] = df['ks'].apply(lambda x: float(x.replace('%', '')) if '%' in x else x)
    df['gs'] = df['gs'].apply(lambda x: float(x.replace('%', '')) if '%' in x else x)

    return df




def data_pipeline(league: str, year:str, season: str, tournament:str, **kwargs):
    data = extract_data(league, year, season, tournament)
    df = create_dataframe(data)
    #df = clean_dataframe(df)
    file_name = f'{league}_{year}_{season}_{tournament}'
    kwargs['ti'].xcom_push(key='file_name', value=file_name)
    file_path = f'/opt/airflow/data/output/{file_name}.csv'
    df.to_csv(rf'{file_path}', sep='\t', index=False, header=False)

    return file_path


class PostgresFileOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 operation,
                 config={},
                 *args,
                 **kwargs):
        super(PostgresFileOperator, self).__init__(*args, **kwargs)
        self.operation = operation
        self.config = config
        self.postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

    def execute(self, context):
        if self.operation == "write":
            self.writeInDb(context)  

    def writeInDb(self, context):
        file_path = context['ti'].xcom_pull(task_ids='extract_data')
        self.postgres_hook.bulk_load(self.config.get('table_name'), file_path)



def database_name(file_name):
    if len(file_name) > 7:
        parts = file_name.split('+')
        initials = ''.join([part[0].upper() for part in parts])
        return initials
    file_name = file_name.replace('+','_')
    return file_name

def table_name (league: str, year:str, season: str, tournament:str):
    tournament = tournament.lower()
    if tournament == "worlds":
        return f'{tournament}_{year}_{season}'
    return f'{database_name(league)}_{year}_{season}_{tournament}'
