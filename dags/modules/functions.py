from dotenv import load_dotenv
import requests
from datetime import datetime
import psycopg2
import os

# Carga de variables de entorno
config_path = './secrets/'
load_dotenv(dotenv_path=f'{config_path}/.env')

tickers = ['bitcoin','ethereum','zcash']

def get_data(endpoint):
    # endpoint the coingecko
    response = requests.get(endpoint)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()  # Parse the JSON response
        data = data['tickers']
    else:
        print(f"Failed to fetch data: {response.status_code}")

    return data

def format_ticker_data(ticker_data):
    return {
        'base': ticker_data.get('base'),
        'target': ticker_data.get('target'),
        'market_name': ticker_data.get('market', {}).get('name'),
        'market_identifier': ticker_data.get('market', {}).get('identifier'),
        'has_trading_incentive': ticker_data.get('market', {}).get('has_trading_incentive', False),
        'last_price': ticker_data.get('last'),
        'volume': ticker_data.get('volume'),
        'converted_last_btc': ticker_data.get('converted_last', {}).get('btc'),
        'converted_last_eth': ticker_data.get('converted_last', {}).get('eth'),
        'converted_last_usd': ticker_data.get('converted_last', {}).get('usd'),
        'converted_last_usd_v2': ticker_data.get('converted_last', {}).get('usd_v2'),
        'converted_volume_btc': ticker_data.get('converted_volume', {}).get('btc'),
        'converted_volume_eth': ticker_data.get('converted_volume', {}).get('eth'),
        'converted_volume_usd': ticker_data.get('converted_volume', {}).get('usd'),
        'converted_volume_usd_v2': ticker_data.get('converted_volume', {}).get('usd_v2'),
        'trust_score': ticker_data.get('trust_score'),
        'bid_ask_spread_percentage': ticker_data.get('bid_ask_spread_percentage'),
        'timestamp': datetime.fromisoformat(ticker_data.get('timestamp')),
        'last_traded_at': datetime.fromisoformat(ticker_data.get('last_traded_at')),
        'last_fetch_at': datetime.fromisoformat(ticker_data.get('last_fetch_at')),
        'is_anomaly': ticker_data.get('is_anomaly', False),
        'is_stale': ticker_data.get('is_stale', False),
        'trade_url': ticker_data.get('trade_url'),
        'coin_id': ticker_data.get('coin_id'),
        'target_coin_id': ticker_data.get('target_coin_id')
    }

def extract_values_as_tuple(formatted_data):
    return (
        formatted_data['base'],
        formatted_data['target'],
        formatted_data['market_name'],
        formatted_data['market_identifier'],
        formatted_data['has_trading_incentive'],
        formatted_data['last_price'],
        formatted_data['volume'],
        formatted_data['converted_last_btc'],
        formatted_data['converted_last_eth'],
        formatted_data['converted_last_usd'],
        formatted_data['converted_last_usd_v2'],
        formatted_data['converted_volume_btc'],
        formatted_data['converted_volume_eth'],
        formatted_data['converted_volume_usd'],
        formatted_data['converted_volume_usd_v2'],
        formatted_data['trust_score'],
        formatted_data['bid_ask_spread_percentage'],
        formatted_data['timestamp'],
        formatted_data['last_traded_at'],
        formatted_data['last_fetch_at'],
        formatted_data['is_anomaly'],
        formatted_data['is_stale'],
        formatted_data['trade_url'],
        formatted_data['coin_id'],
        formatted_data['target_coin_id']
    )

def connect_to_db():
    try:
        # Establish connection with postgres db
        conn = psycopg2.connect(
            dbname=str(os.getenv('POSTGRES_DB')),
            user=str(os.getenv('POSTGRES_USERNAME')),
            password=str(os.getenv('POSTGRES_PASSWORD')),
            host=str(os.getenv('POSTGRES_HOST')),
            port=str(os.getenv('POSTGRES_PORT'))
        )
        
        print("Connected to Postgres database successfully")

        return conn

    except Exception as e:
        print("Error connecting to Postgres database:", e)
        return None

def create_table(conn):
    try:
        command = """
                CREATE TABLE IF NOT EXISTS ticker_luxor (
                    id SERIAL PRIMARY KEY,
                    base VARCHAR(500) NOT NULL,            
                    target VARCHAR(500) NOT NULL,         
                    market_name VARCHAR(500),              
                    market_identifier VARCHAR(500),       
                    has_trading_incentive BOOLEAN,        
                    last_price DECIMAL(20, 8),            
                    volume DECIMAL(30, 8),                
                    converted_last_btc DECIMAL(20, 8),    
                    converted_last_eth DECIMAL(20, 8),    
                    converted_last_usd DECIMAL(20, 8),    
                    converted_last_usd_v2 DECIMAL(20, 8), 
                    converted_volume_btc DECIMAL(30, 8),  
                    converted_volume_eth DECIMAL(30, 8),  
                    converted_volume_usd DECIMAL(30, 8), 
                    converted_volume_usd_v2 DECIMAL(30, 8), 
                    trust_score VARCHAR(500),              
                    bid_ask_spread_percentage DECIMAL(10, 6),
                    timestamp TIMESTAMP WITH TIME ZONE, 
                    last_traded_at TIMESTAMP WITH TIME ZONE, 
                    last_fetch_at TIMESTAMP WITH TIME ZONE,  
                    is_anomaly BOOLEAN,                  
                    is_stale BOOLEAN,                    
                    trade_url TEXT,                       
                    coin_id VARCHAR(500),                 
                    target_coin_id VARCHAR(500)
                    );
                    """

        cur = conn.cursor()
        
        # Execute the SQL command
        cur.execute(command)

        # ommit the transaction
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()
        print("The table already exists or has been succesfully created.")

    except Exception as e:
        print(f"An error occured during the verification of the table: {e}")



