import os
from typing import Union
from fastapi import FastAPI
import sqlite3
from sqlite3 import Error
from dotenv import load_dotenv
import requests
from datetime import datetime
import pandas as pd

app = FastAPI()

load_dotenv()

# Constants for SQLite (database is now a file)
db_path = os.getenv('DB_PATH', 'database.db')  # Default to 'database.db' if not set

lambdas = [0.5, 0.757858283, 0.870550563, 0.933032992, 0.965936329, 0.982820599]
nfs = [1.0000, 1.0000, 1.0000, 1.0000, 1.0020, 1.0462]

def get_binance_data(ticker):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": ticker,
        "interval": "1d",
        "limit": 180
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = pd.DataFrame(response.json(), columns=["Open time", "Open", "High", "Low", "Close", "Volume", "Close time", "Quote asset volume", "Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"])
        data['Close'] = data['Close'].astype(float)  # Convert close prices to float
        return data
    else:
        print("Failed to retrieve data: Status code", response.status_code)
        return None

def calculate_ewma(df, lambdas, nf):
    """Calculate the exponentially weighted moving average."""
    if len(df) < 180:
        print("Not enough data for 180-day window.")
        return None
    
    close_prices = df['Close'].values[::-1]  # Reverse to access t-i
    weights = [(1 - lambdas) * (lambdas ** i) for i in range(180)]
    normalization_factor = 1 / sum(weights)  # Normalize weights
    weighted_sum = sum(weights[i] * close_prices[i] * nf for i in range(len(weights)))
    return normalization_factor * weighted_sum

def sign(x):
    return 1 if x >= 0 else -1

def calculate_trend_indicator(data, lambdas, nfs):
    """Calculate the trend indicator."""
    MA1 = calculate_ewma(data, lambdas[0], nfs[0])  # 1 day
    MA2_5 = calculate_ewma(data, lambdas[1], nfs[1])  # 2.5 days
    MA5 = calculate_ewma(data, lambdas[2], nfs[2])  # 5 days
    MA10 = calculate_ewma(data, lambdas[3], nfs[3])  # 10 days
    MA20 = calculate_ewma(data, lambdas[4], nfs[4])  # 20 days
    MA40 = calculate_ewma(data, lambdas[5], nfs[5])  # 40 days
    MAP1 = sign(MA1 - MA5)
    MAP2 = sign(MA2_5 - MA10)
    MAP3 = sign(MA5 - MA20)
    MAP4 = sign(MA10 - MA40)
    result = MAP1 + MAP2 + MAP3 + MAP4
    return result / 4

def get_trend_indicator(ticker):
    """Fetch data and calculate the trend indicator."""
    data = get_binance_data(ticker)
    if data is not None:
        trend_indicator = calculate_trend_indicator(data, lambdas, nfs)
        return int(trend_indicator)
    return None

# Database connection utility
def connect_to_db():
    """Connect to SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Error as e:
        print(f"SQLite connection error: {e}")
        return None

@app.get("/trend/update")
def fetch_and_update():
    """Fetch and update trend indicators in the database."""
    current_timestamp = datetime.now()
    ticker_eth = "ETHUSDT"
    trend_indicator_eth = get_trend_indicator(ticker_eth)
    ticker_btc = "BTCUSDT"
    trend_indicator_btc = get_trend_indicator(ticker_btc)
    ticker_sol = "SOLUSDT"
    trend_indicator_sol = get_trend_indicator(ticker_sol)

    # Insert data into SQLite
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Create table if not exists (can be skipped if already created)
            cursor.execute('''CREATE TABLE IF NOT EXISTS trend_indicator (
                                bitcoin_trend INTEGER,
                                ethereum_trend INTEGER,
                                solana_trend INTEGER,
                                timestamp TEXT)''')

            # Prepare insert query
            insert_query = "INSERT INTO trend_indicator (bitcoin_trend, ethereum_trend, solana_trend, timestamp) VALUES (?, ?, ?, ?)"
            values = (int(trend_indicator_btc), int(trend_indicator_eth), int(trend_indicator_sol), current_timestamp)

            # Execute insert
            cursor.execute(insert_query, values)
            conn.commit()
        except Error as e:
            print(f"SQLite error: {e}")
        finally:
            conn.close()

@app.get("/trend/check")
def check_current_trend():
    """Fetches the current trend indicator for given tickers from the database."""
    ticker_eth = "ETHUSDT"
    ticker_btc = "BTCUSDT"
    ticker_sol = "SOLUSDT"
    
    # Fetch latest trend data from SQLite
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM trend_indicator ORDER BY timestamp DESC LIMIT 1")
            row = cursor.fetchone()
            if row:
                return {
                    "timestamp": row[3],
                    "trend": {
                        "ethereum": row[1],
                        "bitcoin": row[0],
                        "solana": row[2]
                    }
                }
            else:
                return {"error": "No trend data available"}
        except Error as e:
            return {"error": str(e)}
        finally:
            conn.close()

