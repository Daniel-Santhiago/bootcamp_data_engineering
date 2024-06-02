FROM quay.io/astronomer/astro-runtime:11.3.0

# Compulsory to switch parameter
ENV PIP_USER=false

#python venv setup
RUN python3 -m venv /opt/airflow/venv1

# Install dependencies:
COPY requirements.txt .

# --user   <--- WRONG, this is what ENV PIP_USER=false turns off
#RUN /opt/airflow/venv1/bin/pip install --user -r requirements.txt  <---this is all wrong
RUN /opt/airflow/venv1/bin/pip install -r requirements.txt
ENV PIP_USER=true


ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300
ENV AIRFLOW__CORE__DAG_DIR_LIST_INTERVAL=20
ENV AIRFLOW__CORE__MIN_FILE_PROCESS_INTERVAL=5
ENV AIRFLOW__SCHEDULER__TASK_QUEUED_TIMEOUT=12000

ENV AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE=False
ENV AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=20
ENV AIRFLOW__WEBSERVER__ALLOW_RAW_HTML_DESCRIPTIONS=True

ENV AIRFLOW__SMTP__SMTP_HOST="smtp.gmail.com" \
    AIRFLOW__SMTP__SMTP_PORT="587" \
    AIRFLOW__SMTP__SMTP_SSL="false" \
    AIRFLOW__SMTP__SMTP_STARTTLS="true" \
    AIRFLOW__SMTP__SMTP_MAIL_FROM="Airflow" \
    AIRFLOW__SMTP__SMTP_USER="danieldeveloper01@gmail.com" \
    AIRFLOW__SMTP__SMTP_PASSWORD="ewqbhootahadcjah"