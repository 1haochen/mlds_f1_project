FROM apache/airflow:2.8.1

USER root
COPY dags/requirements.txt /requirements.txt

# Change ownership so airflow user can read it
RUN chown airflow: /requirements.txt

# Switch to airflow user before pip install
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
