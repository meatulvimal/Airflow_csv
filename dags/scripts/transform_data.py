import pandas as pd

def transform_data(ds, **kwargs):
    data_path = '/opt/airflow/data/accidents.csv'
    df = pd.read_csv(data_path)

    # Drop rows with missing values
    df.dropna(inplace=True)

    # Calculate total number of accidents by weather type
    accidents_by_weather = df.groupby('WEATHER')['WEATHER'].count()

    # Store the results in the Airflow context
    kwargs['ti'].xcom_push(key='accidents_by_weather', value=accidents_by_weather.to_json())