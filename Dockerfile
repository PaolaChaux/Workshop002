FROM apache/airflow:2.8.4-python3.9
ADD requirements.txt .
ADD db_config.json .
ADD ./Data .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
COPY requirements.txt .
RUN pip install -r requirements.txt