FROM apache/airflow:3.3.1-python3.12

USER root

RUN apt-get update && apt-get install -y --no-install-recommends build-essential && apt-get clean

USER airflow

COPY requirement.txt .

RUN pip install --no-cache-dir -r requirement.txt
