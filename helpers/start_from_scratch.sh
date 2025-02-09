# Assumes a fresh install of Airflow is desired
docker compose up airflow-init

sleep 10

docker compose up --build --detach