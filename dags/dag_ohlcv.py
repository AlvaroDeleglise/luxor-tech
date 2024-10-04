from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from modules import connect_to_db

def create_or_refresh_view():
    sql_command="""
        CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_view AS
        SELECT 
            base,
            target,
            market_name,
            MIN(last_price) AS open_price,
            MAX(last_price) AS high_price,
            MIN(last_price) AS low_price,
            (ARRAY_AGG(last_price ORDER BY timestamp DESC))[1] AS close_price,
            SUM(volume) AS total_volume,
            DATE_TRUNC('day', timestamp) AS day
        FROM ticker_luxor
        GROUP BY base, target, market_name, day
        ORDER BY day
        WITH DATA;

        -- Refrescar la vista materializada
        REFRESH MATERIALIZED VIEW ohlcv_view;
    """
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute(sql_command)
        conn.commit()
        print("Succesfully created materialized view.")
    except Exception as e:
        print(f"An error occurred: {e}")

# DAG definition
default_args = {
    'owner': 'adeleglise',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 4),
}

dag = DAG(
    'create_materialized_view',
    default_args=default_args,
    description='DAG to create or refresh OHLCV materialized view daily',
    schedule_interval='@daily',  # Daily execution
    catchup=False,
)

tarea_1 = PythonOperator(
    task_id='create_or_refresh_view',
    python_callable=create_or_refresh_view,
    dag=dag
)
# Definir la secuencia de las tareas (en este caso solo hay una)
tarea_1