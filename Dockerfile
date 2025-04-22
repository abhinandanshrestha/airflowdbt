FROM apache/airflow:2.10.5

USER root

# Install dbt dependencies
RUN apt-get update && apt-get install -y git libpq-dev python3-dev && \
    apt-get clean

USER airflow

# Install dbt-core and dbt adapter (choose the one you need)
# For PostgreSQL
RUN pip install --no-cache-dir dbt-core==1.9.4