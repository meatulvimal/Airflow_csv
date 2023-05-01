import json
import pandas as pd

def export_data(ds, **kwargs):
    data_path = '/opt/airflow/data/accidents.csv'

    # Retrieve the accidents by weather data from the Airflow context
    accidents_by_weather = pd.read_json(kwargs['ti'].xcom_pull(key='accidents_by_weather'))

    # Export the results to a new CSV file
    accidents_by_weather.to_csv('/opt/airflow/data/accidents_by_weather.csv')