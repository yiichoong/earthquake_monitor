from airflow.decorators import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


@dag(
    dag_id="usgs_earthquake_dag",
    schedule='@daily',
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=['daily']
)
def usgs_earthquake_dag():

    @task
    def get_request():
        print('Getting request.....')

    @task
    def process_request():
        print('Data process complete!')

    get_request() >> process_request()


usgs_earthquake_dag()
