FROM apache/airflow:2.9.0

COPY requirements.txt /
COPY --chown=airflow:root dags/*.py /opt/airflow/dags
COPY config/news-api-421321-3a5c418f3870.json /config/news-api-421321-3a5c418f3870.json

ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/config/news-api-421321-3a5c418f3870.json"

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
