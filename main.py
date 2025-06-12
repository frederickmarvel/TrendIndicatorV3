import os
from fastapi import FastAPI
from dotenv import load_dotenv
import requests
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2 import DatabaseError

# Load environment variables
load_dotenv()

# Initialize FastAPI application
app = FastAPI()

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "trends_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
}

# Constants for trend calculation
LAMBDAS = [0.5, 0.757858283, 0.870550563, 0.933032992, 0.965936329, 0.982820599]
NFS = [1.0000, 1.0000, 1.0000, 1.0000, 1.0020, 1.0462]


def get_binance_data(ticker: str) -> pd.DataFrame:
    """Fetch historical price data from Binance API."""
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": ticker, "interval": "1d", "limit": 180}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = pd.DataFrame(response.json(), columns=[
            "Open time", "Open", "High", "Low", "Close", "Volume",
            "Close time", "Quote asset volume", "Number of trades",
            "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"
        ])
        data["Close"] = data["Close"].astype(float)
        return data
    except requests.RequestException as e:
        print(f"[ERROR] Failed to fetch data for {ticker}: {e}")
        return None


def calculate_ewma(data: pd.DataFrame, lam: float, nf: float) -> float:
    """Calculate exponentially weighted moving average (EWMA)."""
    if len(data) < 180:
        print("[ERROR] Not enough data for 180-day window.")
        return None

    close_prices = data["Close"].values[::-1]
    weights = [(1 - lam) * (lam ** i) for i in range(180)]
    normalization_factor = 1 / sum(weights)
    weighted_sum = sum(weights[i] * close_prices[i] * nf for i in range(len(weights)))
    return normalization_factor * weighted_sum


def calculate_trend_indicator(data: pd.DataFrame) -> float:
    """Calculate trend indicator based on moving averages."""
    try:
        ma1 = calculate_ewma(data, LAMBDAS[0], NFS[0])  # 1-day EWMA
        ma2_5 = calculate_ewma(data, LAMBDAS[1], NFS[1])  # 2.5-day EWMA
        ma5 = calculate_ewma(data, LAMBDAS[2], NFS[2])  # 5-day EWMA
        ma10 = calculate_ewma(data, LAMBDAS[3], NFS[3])  # 10-day EWMA
        ma20 = calculate_ewma(data, LAMBDAS[4], NFS[4])  # 20-day EWMA
        ma40 = calculate_ewma(data, LAMBDAS[5], NFS[5])  # 40-day EWMA

        indicators = [
            (ma1 - ma5),
            (ma2_5 - ma10),
            (ma5 - ma20),
            (ma10 - ma40),
        ]
        return sum(1 if x >= 0 else -1 for x in indicators) / 4
    except Exception as e:
        print(f"[ERROR] Failed to calculate trend indicator: {e}")
        return None


def connect_to_db():
    """Establish a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except DatabaseError as e:
        print(f"[ERROR] PostgreSQL connection error: {e}")
        return None


@app.get("/trend/update")
def fetch_and_update():
    """Fetch trend data and update the database."""
    current_timestamp = datetime.now()
    tickers = {
        "bitcoin": "BTCUSDT",
        "ethereum": "ETHUSDT",
        "solana": "SOLUSDT",
    }

    indicators = {}
    for name, ticker in tickers.items():
        data = get_binance_data(ticker)
        if data is not None:
            indicators[name] = calculate_trend_indicator(data)
            print(f"[DEBUG] {name.capitalize()} Trend Indicator: {indicators[name]}")
        else:
            print(f"[ERROR] Failed to fetch data for {name}.")
            indicators[name] = None

    if any(indicators[name] is None for name in tickers):
        return {"error": "Failed to calculate one or more trend indicators."}

    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS trend_indicator (
                                bitcoin_trend FLOAT,
                                ethereum_trend FLOAT,
                                solana_trend FLOAT,
                                timestamp TIMESTAMP)''')

            insert_query = '''INSERT INTO trend_indicator (bitcoin_trend, ethereum_trend, solana_trend, timestamp)
                              VALUES (%s, %s, %s, %s)'''
            cursor.execute(insert_query, (
                indicators["bitcoin"], indicators["ethereum"], indicators["solana"], current_timestamp
            ))
            conn.commit()
            print("[DEBUG] Database updated successfully.")
            return {"message": "Trend data updated successfully."}
        except Exception as e:
            print(f"[ERROR] Database error during update: {e}")
            return {"error": "Failed to update the database."}
        finally:
            conn.close()
    else:
        return {"error": "Failed to connect to the database."}


@app.get("/trend/check")
def check_current_trend():
    """Fetch the most recent trend data from the database."""
    conn = connect_to_db()
    if not conn:
        return {"error": "Failed to connect to the database."}

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM trend_indicator ORDER BY timestamp DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            return {
                "timestamp": row[3],
                "trend": {
                    "bitcoin": row[0],
                    "ethereum": row[1],
                    "solana": row[2],
                }
            }
        else:
            return {"error": "No trend data available."}
    except Exception as e:
        print(f"[ERROR] Failed to fetch data: {e}")
        return {"error": "Failed to fetch trend data."}
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
