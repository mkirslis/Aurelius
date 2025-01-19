from config import API_KEY
import logging
import sqlite3
import requests
from datetime import datetime, timezone
import time
import os

PROJECT_NAME = "Aurelius"
VERSION_NUMBER = "0.0.55"
START_DATE = "2023-01-18"
END_DATE = "2025-01-17"
DATABASES = ['stocks.db', 'backup_stocks.db']
TABLES = ['ohlcv_daily']
TICKERS = ['JPM', 'MS',  'GS', 'BAC', 'C', 'WFC']


def output(message, log_level="info"):
    """
    Prints & logs a message simultaneously.
    # Logging Levels:
    # Level      String       Value           Description
    # DEBUG      "debug"      10              Detailed debugging information.
    # INFO       "info"       20              General operational messages.
    # WARNING    "warning"    30              Something unexpected or future problems.
    # ERROR      "error"      40              Serious problem, some functionality failed.
    # CRITICAL   "critical"   50              Severe error, program may not continue.
    """
    log_levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    log_level_numeric = log_levels.get(log_level.lower(), logging.INFO)
    print(message)
    logging.log(log_level_numeric, message)


def aggregate_daily_bars(connection, url, database, table, ticker):
    """Pulls daily OHLCV data from Polygon IO."""
    time.sleep(13)  # API limits 5 requests per minute
    connection = sqlite3.connect(f"{database}")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        cursor = connection.cursor()
        for result in data['results']:
            dt = datetime.fromtimestamp(result['t'] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(f'''
                INSERT OR IGNORE INTO {table} (timestamp, datetime, ticker, open, high, low, close, volume, volume_weighted, trades, resultsCount, request_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                result['t'],
                dt,
                data['ticker'],
                result['o'],
                result['h'],
                result['l'],
                result['c'],
                result['v'],
                result['vw'],
                result['n'],
                data['resultsCount'],
                data['request_id']
            ))
        connection.commit()
        connection.close()
        return data
    else:
        return f"Error: {response.status_code}"


def main():
    """Logging"""
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(filename=f"logs/log_v{VERSION_NUMBER}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    """Start"""
    start_time = datetime.now()
    start_datetime = start_time.strftime("%Y-%m-%d %H:%M:%S")
    output(f"{PROJECT_NAME} v{VERSION_NUMBER}")
    output(f"Program started: {start_datetime}")

    """Initialization"""
    for database in DATABASES:
        for table in TABLES:
            try:
                connection = sqlite3.connect(f"{database}")
                cursor = connection.cursor()
                output(f"Initializing {database} & {table}...")
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlcv_daily (
                        timestamp INTEGER,
                        datetime DATETIME,
                        ticker TEXT,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume INTEGER,
                        volume_weighted REAL,
                        trades INTEGER,
                        resultsCount INTEGER,
                        request_id TEXT )
                    ''')
                output(f"{database} & {table} have been initialized.")
            except sqlite3.Error as e:
                output(f"Error initializing {database} & {table}: {e}")
            finally:
                cursor.close()

    """Procurement"""
    for database in DATABASES:
        for table in TABLES:
            for ticker in TICKERS:
                url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{START_DATE}/{END_DATE}?apiKey={API_KEY}"
                output(f"Procuring data for {database}, {table}, {ticker}...")
                aggregate_daily_bars(connection, url, database, table, ticker)
                output(f"Data procured for {database}, {table}, {ticker}.")

    """End"""
    end_time = datetime.now()
    runtime = end_time - start_time
    end_datetime = end_time.strftime("%Y-%m-%d %H:%M:%S")
    output(f"Program ended: {end_datetime}")
    output(f"Runtime: {runtime}")


if __name__ == "__main__":
    main()
