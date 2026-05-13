# from airflow import dag
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess

@dag(
    start_date = datetime(2026,5,3),
    schedule="@daily",
    catchup=False,
    tags=["my_dag"]
)
def my_dag():

    @task(
        retries= 3,
        retry_delay= timedelta(minutes=5)
    )
    def bronze():
        subprocess.run(["python", "/opt/airflow/project/bronze/brz.py"], check=True)


    @task(
        retries= 3,
        retry_delay= timedelta(minutes=5)
    )
    def silver():
        subprocess.run(["python", "/opt/airflow/project/silver/slv_dims.py"], check=True)
        subprocess.run(["python", "/opt/airflow/project/silver/slv_fact.py"], check=True)
    
    @task.bash
    def dbt():
        return "cd /opt/airflow/project/cc_pipeline_dbt && dbt build --profiles-dir /opt/airflow/project/cc_pipeline_dbt"


    dbt() << silver() << bronze()

my_dag()