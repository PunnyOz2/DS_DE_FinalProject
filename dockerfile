FROM apache/airflow:2.9.0

COPY requirements.txt /
EXPOSE 8501
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt