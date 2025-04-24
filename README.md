# Running dbt in isolation
## Step1:
```
pip install dbt-core dbt-postgres
```

## Step 2: create packages.yml file inside your dbt project, these packages are required for introducing surrogate keys
```
packages:
  - package: dbt-labs/dbt_utils
    version: [">=0.8.0", "<1.0.0"]
```

## Step 3 : Run
```
dbt deps
```
### SQL For Simulation
```
create table weather (
	id serial primary key,
	data text,
	created_at_utc timestamp
);

INSERT INTO public.weather
(id, "data", created_at_utc)
VALUES(nextval('weather_id_seq'::regclass), 'test1', now()),
(nextval('weather_id_seq'::regclass), 'test2', now()-interval '1 min'),
(nextval('weather_id_seq'::regclass), 'test3', now()-interval '2 min'),
(nextval('weather_id_seq'::regclass), 'test4', now()-interval '3 min');


select 
	*
from weather w ;

select 
	*
from transformed."temp" t 
```


# Running dbt with airflow
# Step 1:  Clone the repo
```
git clone https://github.com/abhinandanshrestha/airflowdbt
cd airflowdbt/
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

# Step2:
```
docker compose up -d
```

# Step3:
```
docker exec -it <worker_container> bash
cd weather/
dbt debug
```



