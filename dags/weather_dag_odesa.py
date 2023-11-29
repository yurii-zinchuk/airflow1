from airflow import DAG
from airflow.models import Variable
from datetime import datetime
import json
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from airflow.providers.http.operators.http import HttpOperator


def _process_temperature(ti):
    info = ti.xcom_pull(task_ids="extract_data")
    timestamp = info["data"][0]["dt"]
    temp = info["data"][0]["temp"]
    return timestamp, temp


def _process_humidity(ti):
    info = ti.xcom_pull(task_ids="extract_data")
    timestamp = info["data"][0]["dt"]
    humidity = info["data"][0]["humidity"]
    return timestamp, humidity


def _process_cloudiness(ti):
    info = ti.xcom_pull(task_ids="extract_data")
    timestamp = info["data"][0]["dt"]
    clouds = info["data"][0]["clouds"]
    return timestamp, clouds


def _process_wind_speed(ti):
    info = ti.xcom_pull(task_ids="extract_data")
    timestamp = info["data"][0]["dt"]
    wind_speed = info["data"][0]["wind_speed"]
    return timestamp, wind_speed


with DAG(
    dag_id="weather_dag",
    start_date=datetime(2023, 11, 27),
    catchup=True,
    schedule_interval="@daily",
) as dag:
    extract_coords = HttpOperator(
        task_id="extract_coords",
        method="GET",
        endpoint="geo/1.0/direct",
        http_conn_id="weather_api",
        data={"q": "Odesa", "appid": Variable.get("API_KEY")},
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    extract_data = HttpOperator(
        task_id="extract_data",
        method="GET",
        endpoint="data/3.0/onecall/timemachine",
        http_conn_id="weather_api",
        data={
            "lat": "{{ti.xcom_pull(task_ids='extract_coords')[0][\"lat\"]}}",
            "lon": "{{ti.xcom_pull(task_ids='extract_coords')[0][\"lon\"]}}",
            "dt": "{{execution_date}}",
            "appid": Variable.get("API_KEY"),
        },
        response_filter=lambda response: json.loads(response.text),
        do_xcom_push=True,
        log_response=True,
    )

    process_temperature = PythonOperator(
        task_id="process_temperature", python_callable=_process_temperature
    )

    process_wind_speed = PythonOperator(
        task_id="process_wind_speed", python_callable=_process_wind_speed
    )

    process_humidity = PythonOperator(
        task_id="process_humidity", python_callable=_process_humidity
    )

    process_cloudiness = PythonOperator(
        task_id="process_cloudiness", python_callable=_process_cloudiness
    )

    inject_data = SqliteOperator(
        task_id="inject_data",
        sqlite_conn_id="airflow_db",
        sql="""
            INSERT INTO measures (city, timestamp, temp, cloudiness, wind, humidity) VALUES
            (
            "Kharkiv",
            {{ti.xcom_pull(task_ids='process_temperature')[0]}},
            {{ti.xcom_pull(task_ids='process_temperature')[1]}},
            {{ti.xcom_pull(task_ids='process_cloudiness')[1]}},
            {{ti.xcom_pull(task_ids='process_wind_speed')[1]}},
            {{ti.xcom_pull(task_ids='process_humidity')[1]}});
            """,
    )

    (
        extract_coords
        >> extract_data
        >> [
            process_temperature,
            process_wind_speed,
            process_humidity,
            process_cloudiness,
        ]
        >> inject_data
    )
