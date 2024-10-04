from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from dotenv import load_dotenv
import os
from datetime import datetime
import time
# import requests
# import sys
from modules import (
    get_data,
    format_ticker_data,
    create_table,
    connect_to_db,
    extract_values_as_tuple
    )

# List of ticker_ids
ticker_ids = ['bitcoin', 'ethereum', 'zcash']

def check_table_exists():
    print("Conectando a Postgres")
    conn = connect_to_db()
    print("Chequeando existencia de tabla de destino")
    create_table(conn)

def get_api_data(ticker_id: str) -> list:
    print(f'Obteniendo data de: {ticker_id}')
    # endpoint the coingecko
    endpoint = f"https://api.coingecko.com/api/v3/coins/{ticker_id}/tickers"
    data = get_data(endpoint)
    return data

def ingest_data(**context):
    # Get data from coingecko API
    for i in range((60*5)-1):
        data = context['task_instance'].xcom_pull(
            task_ids="api_ingestion.get_api_data",
            include_prior_dates=False
            )

        data_formatted = []
        
        for coin in data:
            if coin:
                # format and ingest data to postgres
                # data_formatted = [extract_values_as_tuple(format_ticker_data(i)) for i in data]
                data_formatted.extend([extract_values_as_tuple(format_ticker_data(i)) for i in coin])

        print(len(data_formatted))

        # SQL command for data insertion
        sql_insert = """
            INSERT INTO ticker_luxor (
                base,
                target,
                market_name,
                market_identifier,
                has_trading_incentive,
                last_price,
                volume,
                converted_last_btc,
                converted_last_eth,
                converted_last_usd,
                converted_last_usd_v2,
                converted_volume_btc,
                converted_volume_eth,
                converted_volume_usd,
                converted_volume_usd_v2,
                trust_score,
                bid_ask_spread_percentage,
                timestamp,
                last_traded_at,
                last_fetch_at,
                is_anomaly,
                is_stale,
                trade_url,
                coin_id,
                target_coin_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        if data_formatted:
            conn = connect_to_db()
            cursor = conn.cursor()

            try:
                # Execute the instruction with multiple rows
                cursor.executemany(sql_insert, data_formatted)

                # Commit changes
                conn.commit()
                print("Data was succesfully inserted in Postgres table.")

            except Exception as e:
                print(f"An error occurred: {e}")
                conn.rollback()  # Deshacer cualquier cambio si hay un error

            finally:
                # Close cursor and conexion
                cursor.close()
                conn.close()

        time.sleep(1)

# DAG definition
default_args = {
    'owner' : 'adeleglise',
    'start_date': datetime(2024, 10, 4),  # Start date
    'retries': 5  # Number of retries
}

with DAG(dag_id='ingest_data',
         description="ingest coingecko data into postgres",
         tags=["ingest","etl","coingecko","postgres"],
         default_args=default_args,
         schedule_interval='*/5 * * * *',  # Execution interval
         catchup=False) as dag:

    tarea_1 = PythonOperator(
        task_id='check_table_exists',
        python_callable=check_table_exists
    )

    # Create task group for data ingestion
    with TaskGroup(group_id='api_ingestion') as api_ingestion_group:
        # Map extraction task for each ticker_id
        get_api_tasks = PythonOperator.partial(
            task_id='get_api_data',
            python_callable=get_api_data
        ).expand(op_kwargs=[{'ticker_id': ticker_id} for ticker_id in ticker_ids])

    tarea_3 = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
        # op_kwargs={'ticker_id': 'bitcoin'}
        provide_context=True
    )

    # Definition of task sequence
    tarea_1 >> api_ingestion_group >> tarea_3