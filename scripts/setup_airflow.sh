#!/bin/bash

echo "=========================================="
echo "Setting up Apache Airflow"
echo "=========================================="

mkdir -p ./dags
mkdir -p ./logs
mkdir -p ./plugins
mkdir -p ./config

echo "AIRFLOW_UID=$(id -u)" > .env.airflow

cat > airflow.cfg << EOF
[core]
dags_folder = /opt/airflow/dags
hostname_callable = socket.getfqdn
default_timezone = utc
executor = LocalExecutor
parallelism = 32
max_active_tasks_per_dag = 16
dags_are_paused_at_creation = False
max_active_runs_per_dag = 16

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow

[webserver]
web_server_port = 8080
web_server_host = 0.0.0.0
base_url = http://localhost:8081

[scheduler]
job_heartbeat_sec = 5
scheduler_heartbeat_sec = 5
num_runs = -1

[logging]
logging_level = INFO
fab_logging_level = WARN

[smtp]
smtp_host = localhost
smtp_starttls = True
smtp_ssl = False
smtp_port = 587
smtp_mail_from = airflow@example.com
EOF

echo "Airflow configuration created successfully!"

echo "Airflow setup complete!"
echo ""