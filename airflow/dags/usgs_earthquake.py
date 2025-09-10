import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from operators.postgres_db_operator import PostgresLoadOperator

from airflow.decorators import dag, task


@dag(
    dag_id="usgs_earthquake_dag",
    schedule="@daily",
    start_date=datetime(2025, 8, 1),
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "depends_on_past": False,
    },
    catchup=False,
    tags=["daily"],
)
def usgs_earthquake_dag():
    base_dir = Path("/opt/airflow")
    output_dir = base_dir / "data" / "earthquake"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"earthquake_data_{timestamp}.json"

    @task
    def process_earthquake_data(file_path: str):
        try:
            with open(file_path) as f:
                data = json.load(f)

            records = []

            for f in data["features"]:
                props = f["properties"]
                geom = f["geometry"]["coordinates"]
                records.append(
                    {
                        "id": f["id"],
                        "mag": props["mag"],
                        "place": props["place"],
                        "time": props["time"],
                        "updated": props["updated"],
                        "tz": props["tz"],
                        "url": props["url"],
                        "detail": props["detail"],
                        "felt": props["felt"],
                        "cdi": props["cdi"],
                        "mmi": props["mmi"],
                        "alert": props["alert"],
                        "status": props["status"],
                        "tsunami": props["tsunami"],
                        "sig": props["sig"],
                        "net": props["net"],
                        "code": props["code"],
                        "ids": props["ids"],
                        "sources": props["sources"],
                        "types": props["types"],
                        "nst": props["nst"],
                        "dmin": props["dmin"],
                        "rms": props["rms"],
                        "gap": props["gap"],
                        "mag_type": props["magType"],
                        "type": props["type"],
                        "title": props["title"],
                        "longitude": geom[0],
                        "latitude": geom[1],
                        "depth": geom[2],
                    }
                )

            df = pd.DataFrame(records)

            logging.info(f"Total records: {len(df)}")
            logging.info(f"\n{df}")
        except Exception as e:
            logging.error(f"Error loading from {file_path}: {e}")

        try:
            temp_file_path = output_dir / "temp_usgs_earthquake_event.csv"
            df.to_csv(temp_file_path, index=False)
        except Exception as e:
            logging.error(f"Error creating temporary csv: {e}")

        return str(temp_file_path)

    @task
    def cleanup_after_load(file_path: str):
        os.remove(file_path)

    # Step 1: Extract data from API
    # fetch_earthquake_data = ApiRequestOperators(
    #     task_id="fetch_earthquake_data",
    #     endpoint="/summary/all_hour.geojson",
    #     http_conn_id="usgs_api",
    #     output_dir=output_dir,
    #     output_filename=filename,
    # )

    # Step 2: Preprocess the API response and put into temporary file
    # process_earthquake_data_task = process_earthquake_data(fetch_earthquake_data.output)
    process_earthquake_data_task = process_earthquake_data(
        "/opt/airflow/data/earthquake/earthquake_data_20250909095923.json"
    )

    # Step 3: Load the data into Postgres database
    load_data_into_postgres = PostgresLoadOperator(
        task_id="load_data_into_postgres",
        conn_id="postgres_conn_id",
        file_path=process_earthquake_data_task,
        table_name="usgs.stg_earthquake_event",
        trunc_table=True,
    )

    # Step 4: Remove the temporary file
    cleanup_after_load_task = cleanup_after_load(process_earthquake_data_task)

    # fetch_earthquake_data >>
    process_earthquake_data_task >> load_data_into_postgres >> cleanup_after_load_task


usgs_earthquake_dag()
