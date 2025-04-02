FROM apache/airflow:latest

# Switch to root user to install system dependencies
USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean 

# Switch to airflow user before installing Python packages
USER airflow
RUN pip install --no-cache-dir kagglehub matplotlib pandas sqlalchemy psycopg2-binary
