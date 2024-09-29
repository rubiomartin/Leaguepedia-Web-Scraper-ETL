import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.etl_functions import extract_data, create_dataframe, clean_dataframe, file_name

def data_pipeline(url, league: str, year:str, tournament:str, **kwargs):
    data = extract_data(url, league, year, tournament)
    df = create_dataframe(data)
    df = clean_dataframe(df)
    fileName = file_name(league, year, tournament)
    kwargs['ti'].xcom_push(key='file_name', value=fileName)
    file_path = f'/opt/airflow/data/output/{fileName}.csv'
    df.to_csv(rf'{file_path}', sep='\t', index=False, header=False)

    return file_path