```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

```
docker compose up -d
```

```
pip install dbt-core dbt-postgres
```

```
docker exec -it <worker_container> bash
cd weather/
dbt debug
```

