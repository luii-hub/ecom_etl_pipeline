FROM apache/airflow:3.1.5

ENV SHELL /bin/bash

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} --no-cache-dir -r requirements.txt