import json
from pathlib import Path
from airflow.decorators import dag, task
from operators.api_request_operator import ApiRequestOperators
from datetime import datetime, timedelta


@dag(
    dag_id='usgs_earthquake_dag',
    schedule='@daily',
    start_date=datetime(2025, 8, 1),
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
        'depends_on_past': False,
    },
    catchup=False,
    tags=['daily']
)
def usgs_earthquake_dag():

    base_dir = Path("/opt/airflow")
    output_dir = base_dir / 'data' / 'earthquake'
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = output_dir / f'earthquake_data_{timestamp}.json'


    @task
    def process_earthquake_data():
        if filename.exists():
            with open(filename) as f:
                data = json.load(f)
            print(f"Loaded {len(data.get('features', []))} earthquake events.")
            print(f"Saving file to: {filename.resolve()}")
        else:
            print("File not found!")


    # Step 1: Extract data from API
    fetch_earthquake_data = ApiRequestOperators(
        task_id="fetch_earthquake_data",
        endpoint='/summary/all_hour.geojson',
        http_conn_id='usgs_api',
        output_dir=output_dir,
        output_filename=filename
    )


    # Step 2: Print API response
    process_earthquake_data_task = process_earthquake_data()


    fetch_earthquake_data >> process_earthquake_data_task


usgs_earthquake_dag()
