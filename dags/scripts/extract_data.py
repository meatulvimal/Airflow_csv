import pandas as pd

def extract_data():
    df = pd.read_csv('https://data.seattle.gov/api/views/vbm8-yswv/rows.csv?accessType=DOWNLOAD')
    df.to_csv('/opt/airflow/data/accidents.csv', index=False)