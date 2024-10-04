from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from modules import connect_to_db
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json
from decimal import Decimal

# Load env variables
config_path = './secrets/'
load_dotenv(dotenv_path=f'{config_path}/.env')

def compute_stats():
    # Connect to Postgres
    conn = connect_to_db()
    cursor = conn.cursor()

    # Make query with aggregate functions:
    # computes avg volume and price for the
    # last 5 minutes
    cursor.execute("""
                    SELECT 
                        base, 
                        target, 
                        market_name,
                        AVG(volume) AS avg_volume,
                        AVG(last_price) AS avg_price
                    FROM 
                        ticker_luxor
                   WHERE 
                        timestamp >= NOW() - INTERVAL '5 minutes'
                    GROUP BY 
                        base, 
                        target, 
                        market_name;
                    """)

    cursor.execute("""
                    WITH current_period AS (
                        -- Promedios de los últimos 5 minutos
                        SELECT 
                            base, 
                            target, 
                            market_name,
                            AVG(volume) AS avg_volume,
                            AVG(last_price) AS avg_last_price
                        FROM 
                            ticker_luxor
                        WHERE 
                            timestamp >= NOW() - INTERVAL '5 minutes' 
                            AND timestamp < NOW()
                        GROUP BY 
                            base, 
                            target, 
                            market_name
                    ), previous_period AS (
                        -- Promedios de los 5 minutos anteriores a los últimos 5 minutos
                        SELECT 
                            base, 
                            target, 
                            market_name,
                            AVG(volume) AS avg_volume_prev,
                            AVG(last_price) AS avg_last_price_prev
                        FROM 
                            ticker_luxor
                        WHERE 
                            timestamp >= NOW() - INTERVAL '10 minutes'
                            AND timestamp < NOW() - INTERVAL '5 minutes'
                        GROUP BY 
                            base, 
                            target, 
                            market_name
                    )
                    SELECT 
                        c.base, 
                        c.target, 
                        c.market_name,
                        c.avg_volume,
                        c.avg_last_price,
                        -- Cambio porcentual del volumen
                        CASE 
                            WHEN p.avg_volume_prev IS NULL THEN 0.0 
                            ELSE ((c.avg_volume - p.avg_volume_prev) / p.avg_volume_prev) * 100 
                        END AS percent_change_volume,
                        -- Cambio porcentual del last_price
                        CASE 
                            WHEN p.avg_last_price_prev IS NULL THEN 0.0
                            ELSE ((c.avg_last_price - p.avg_last_price_prev) / p.avg_last_price_prev) * 100 
                        END AS percent_change_last_price
                    FROM 
                        current_period c
                    LEFT JOIN 
                        previous_period p
                        ON c.base = p.base 
                        AND c.target = p.target 
                        AND c.market_name = p.market_name;
                    """
                )

    # Get aggrated results
    resultados = cursor.fetchall()
    # Get list of columns names
    column_names = [desc[0] for desc in cursor.description]

    # Close conexion
    cursor.close()
    conn.close()

    # Config radpanda producer
    producer = KafkaProducer(
        bootstrap_servers=str(os.getenv('REDPANDA_HOST')),
        client_id=str(os.getenv('REDPANDA_CLIENT_ID'))
        )

    def on_success(metadata):
        print(f"Sent to topic '{metadata.topic}' at offset {metadata.offset}")

    def on_error(e):
        print(f"Error sending message: {e}")

    # Function to convert decimals to float
    def decimal_default(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError("Object of type %s is not JSON serializable" % type(obj))

    # Send messages to redpanda topic
    TOPIC = "tickers_agg_luxor"
    THRESHOLD = 2

    for msg in resultados:
        # Format message as json
        msg = {k:v for k,v in zip(column_names,msg)}
        # Define message key
        msg_key = f"{msg.get('base')}|{msg.get('target')}|{msg.get('market_name')}"
        coin_name = msg.get('base')
        print(msg_key)
        # Get change in volume and price
        percent_change_volume = abs(msg.get('percent_change_volume'))
        percent_change_price = abs(msg.get('percent_change_last_price'))

        msg = json.dumps(msg,default=decimal_default)

        # Save message in topic
        future = producer.send(
            TOPIC,
            key=msg_key.encode('utf-8'),
            value = msg.encode('utf-8')
            )
        future.add_callback(on_success)
        future.add_errback(on_error)

        # Send alert of price change
        if percent_change_price >= THRESHOLD:
            print(f'Alert: a significant percentual change in price was detected for {coin_name}!')
            # Save message in topic with anomalies
            future = producer.send(
                f'{TOPIC}_price_anomalies',
                key=msg_key.encode('utf-8'),
                value = msg.encode('utf-8')
                )
            future.add_callback(on_success)
            future.add_errback(on_error)

        # Send alert of volume change
        if percent_change_volume >= THRESHOLD:
            print(f'Alert: a significant percentual change in volume was detected for {coin_name}!')
            # Save message in topic with anomalies
            future = producer.send(
                f'{TOPIC}_volume_anomalies',
                key=msg_key.encode('utf-8'),
                value = msg.encode('utf-8')
                )
            future.add_callback(on_success)
            future.add_errback(on_error)

    producer.flush()
    producer.close()

# DAG configuration
default_args = {
    'owner' : 'adeleglise',
    'start_date': datetime(2024, 10, 4),
    'retries': 5
}

# DAG execution
with DAG(dag_id='monitoring_system',
         description="identify anomalies",
         tags=["anomaly","redpanda"],
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:

    tarea_1 = PythonOperator(
        task_id='compute_statistics',
        python_callable=compute_stats
        )
    
    tarea_1
    
    
