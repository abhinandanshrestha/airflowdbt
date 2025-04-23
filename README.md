# Step1:
```
pip install dbt-core dbt-postgres
```

# Step2: 
```
git clone https://github.com/abhinandanshrestha/airflowdbt
cd airflowdbt/
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
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

# Step3:
```
docker compose up -d
```

# Step4:
```
docker exec -it <worker_container> bash
cd weather/
dbt debug
```



