# Assumes a restart of an existing Airflow is desired
docker compose up airflow-init

sleep 10

docker compose up --detach