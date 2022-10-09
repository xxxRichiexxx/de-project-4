import requests
import psycopg2
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook


api_conn = BaseHook.get_connection('API')
dwh_conn = BaseHook.get_connection('DWH')

HEADERS = json.loads(api_conn.extra)

URL = api_conn.host

DWH_CONN = {
    'host': dwh_conn.host,
    'port': dwh_conn.port,
    'dbname': 'de',
    'user': dwh_conn.login,
    'password': dwh_conn.password,
}

DATA_TYPES = ['restaurants', 'couriers', 'deliveries']


def get_data(data_type, **context):

    offset = 0
    data = []

    while True:
        url = URL+data_type+f'/?offset={offset}'
        if data_type == 'deliveries':
            url += f"&from={context['ds']} 00:00:00&to{context['ds']} 23:59:59"
        response = requests.get(
            url,
            headers=HEADERS
        )
        response.raise_for_status()
        data_on_page = response.json()
        if not data_on_page:
            break
        data.extend(response.json())
        offset += 50

    with psycopg2.connect(**DWH_CONN) as conn:
        cur = conn.cursor()
        query = f"""
                INSERT INTO stg.{data_type} (object_value, update_ts)
                VALUES (%s, %s)
                ON CONFLICT (update_ts)
                DO UPDATE SET object_value = EXCLUDED.object_value;
                """
        cur.execute(
            query,
            (json.dumps(data, ensure_ascii=False), context['ds'])
        )
        conn.commit()


with DAG(
    dag_id='deliveries_dag',
    default_args={'owner': 'xxxRichiexxx'},
    schedule_interval='@daily',
    start_date=days_ago(1)
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("STAGE") as stage:

        get_data_tasks = []

        for item in DATA_TYPES:
            get_data_tasks.append(
                PythonOperator(
                    task_id=f'get_{item}',
                    python_callable=get_data,
                    op_kwargs={'data_type': item}
                )
            )

    with TaskGroup("DDS-pt1") as dds_pt1:
        update_couriers = PostgresOperator(
            task_id='update_dds_couriers',
            postgres_conn_id='DWH',
            sql="sql/update_dds_couriers.sql"
        )

        update_calendar = PostgresOperator(
            task_id='update_dds_calendar',
            postgres_conn_id='DWH',
            sql="sql/update_dds_calendar.sql"
        )

        [update_couriers, update_calendar]

    with TaskGroup("DDS-pt2") as dds_pt2:

        update_orders = PostgresOperator(
            task_id='update_dds_orders',
            postgres_conn_id='DWH',
            sql="sql/update_dds_orders.sql"
        )

        update_deliveries = PostgresOperator(
            task_id='update_dds_deliveries',
            postgres_conn_id='DWH',
            sql="sql/update_dds_deliveries.sql"
        )

        update_orders >> update_deliveries

    update_mart = PostgresOperator(
            task_id='update_mart',
            postgres_conn_id='DWH',
            sql="sql/update_cdm_dm_courier_ledger.sql",
        )

    end = EmptyOperator(task_id="end")

    start >> stage >> dds_pt1 >> dds_pt2 >> update_mart >> end
