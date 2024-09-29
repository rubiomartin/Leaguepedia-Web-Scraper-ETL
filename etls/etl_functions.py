import pandas as pd
import requests
from bs4 import BeautifulSoup
import sys
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def construct_url(league, year, tournament):
    if tournament.lower() == "worlds":
        if len(year) == 1:
            return f"https://lol.fandom.com/wiki/Special:RunQuery/TournamentStatistics?TS%5Btournament%5D=World+Championship+Season+{year}&TS%5Bpreload%5D=TournamentByPlayer&_run="
        else:
            return f"https://lol.fandom.com/wiki/Special:RunQuery/TournamentStatistics?TS%5Btournament%5D={year}+Season+World+Championship%2F{tournament}&TS%5Bpreload%5D=TournamentByPlayer&_run="
    return f"https://lol.fandom.com/wiki/Special:RunQuery/TournamentStatistics?TS%5Btournament%5D={league}%2F{year}+Season%2F{tournament}+Season&TS%5Bpreload%5D=TournamentByPlayer&_run="


def extract_data(url=None, league=None, year=None, tournament=None):
    try:
        url = url or construct_url(league, year, tournament)
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find_all('tbody')[0]        
        if table:
            return table
        else:
            print("No table found on the page")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


def create_dataframe(table):
    df_columns = ['teams', 'player', 'games', 'wins', 'loses', 'winrate', 'k', 'd', 'a', 
                  'kda', 'cs', 'csm', 'g', 'gm', 'dmg', 'dmgm', 'kpar', 'ks', 
                  'gs', 'champs_played']    
    df = pd.DataFrame(columns=df_columns)
    rows = table.find_all('tr')
    for row_index in range(5, len(rows)):
        cells = rows[row_index].find_all('td')
        row_data = [cells[0].find('a').get('title')] + [cell.text for cell in cells[1:-1]]
        df.loc[len(df)] = row_data
    span_elements = [s for s in table.find_all('span') if 'title' in s.attrs]
    champs_names = []
    df['champs_played'] = df['champs_played'].astype(int)  # Convert 'champs_played' column to integers
    i = 0
    for _, row in df.iterrows():
        # Select titles based on the number of champions ('champs_played')
        titles_count = min(row['champs_played'], 3)
        selected_titles = span_elements[i:i + titles_count]
        champs = ", ".join([title['title'] for title in selected_titles])
        champs_names.append(champs)
        i += titles_count

    df['champs_names'] = champs_names

    return df


def clean_nan_columns(df):
    columns = ['games','wins','loses','winrate','k','d','a','kda','cs','csm','g','gm','dmg','dmgm','kpar','ks','gs','champs_played']
    for column in columns:
        df[column] = df[column].apply(lambda x: x.replace('-nan%', '\\N') if isinstance(x, str) and '-nan%' in x else x)
    return df
	
def clean_percentage_columns(df):
    columns = ['games','wins','loses','winrate','k','d','a','kda','cs','csm','g','gm','dmg','dmgm','kpar','ks','gs','champs_played']
    for column in columns:
        df[column] = df[column].apply(lambda x: (x.replace('%', '')) if isinstance(x, str) and '%' in x else x)
        df[column] = df[column].apply(lambda x: (x.replace('-', '\\N')) if isinstance(x, str) and '-' in x else x)

    return df


# Clean columns in case of nulls
def clean_dataframe (df):
    clean_nan_columns(df)
    clean_percentage_columns(df)
    df['dmg'] = df['dmg'].apply(lambda x: float(x.replace('k', '')) if 'k' in x else x)
    df['g'] = df['g'].apply(lambda x: float(x.replace('k', '')) if 'k' in x else x)
  

    return df



# Operator for connect the Postgresql database
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


def file_name(league: str, year: str, tournament: str):
    if len(league) > 11:
        league_processed = ''.join([part[0].upper() for part in league.split('+')])
    else:
        league_processed = league.replace('+', '_').replace('-', '_')

    if tournament.lower() == "worlds":
        return f'{tournament}_{year}' if len(year) == 1 else f'{tournament}_{year}_{league_processed}'
    
    return f'{league_processed}_{year}_{tournament}'


