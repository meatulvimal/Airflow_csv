FROM apache/airflow:2.2.3-python3.9

USER root

RUN apt-get update && \
    apt-get install -y gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir pandas

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV AIRFLOW_HOME=/opt/airflow

COPY dags/ /opt/airflow/dags/

# Add admin user
RUN airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Initialize the database
RUN airflow db init

# Create the webserver and scheduler processes
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["bash", "-c", "airflow webserver & airflow scheduler"]