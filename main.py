from config import *
from databases import *
from datetime import datetime, timezone
import time
import sqlite3
import os
import pandas as pd
import pytz
import logging
import requests

PROJECT_NAME = "Aurelius"
VERSION_NUMBER = "0.0.4"
EARLIEST_MARKET_DATE = "2023-01-17"
LATEST_MARKET_DATE = "2025-01-15"

logging.basicConfig(filename=f"log_v{VERSION_NUMBER}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def output(message, log_level="info"):
    """
    Print & log simultaneously.

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


def check_database_exists(database: str) -> bool:
    """Check if a database file exists."""
    try:
        if os.path.exists(database):
            output(f"Database '{database}' already exists.")
            return True
        else:
            output(f"Database '{database}' does not exist.")
            return False
    except OSError as e:
        output(f"Error checking for database '{database}': {e}", "error")
        return True


def create_database(database: str):
    """Create a database file."""
    try:
        with sqlite3.connect(database):
            output(f"Database '{database}' created.")
    except sqlite3.Error as e:
        output(f"Error creating database '{database}': {e}", "error")


def check_table_exists(connection: sqlite3.Connection, database: str, table_name: str) -> bool:
    """Check if a table exists in a database."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()
        if result:
            output(f"Table '{database}, {table_name}' already exists.")
            return True
        else:
            output(f"Table '{database}, {table_name}' does not exist.")
            return False
    except sqlite3.Error as e:
        output(f"Error checking table '{database}, {table_name}': {e}", "error")
        return True
    finally:
        cursor.close()


def create_table(connection: sqlite3.Connection, database: str, table_name: str, query: str):
    """Create a table in a database."""
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        output(f"Table '{database}, {table_name}' created.")
    except sqlite3.Error as e:
        output(f"Error creating table '{database}, {table_name}': {e}", "error")
    finally:
        cursor.close()


def aggregate_bars_baseline(url):
    """Pulls timestamps for $SPY aggregates to compare against other tickers."""
    time.sleep(13)  # API limits 5 requests per minute
    
    # If any old timestamps exist before the most recent current date
    # Keep them by removing all timestamps greater than latest date before pull

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        connection = sqlite3.connect("BaselineTimestamps.db")
        cursor = connection.cursor()
        for result in data['results']:
            cursor.execute('''
                INSERT INTO baseline_timestamps (timestamp)
                VALUES (?)
            ''', (
                result['t'],
            ))
        connection.commit()
        connection.close()
        output("Pulled baseline timestamps from $SPY.")
        return data
    else:
        return f"Error: {response.status_code}"


def aggregate_bars(url, database, table):
    """Pulls daily OHLCV data from Polygon IO."""
    time.sleep(13) # API limits 5 requests per minute
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        connection = sqlite3.connect(f"{database}")
        cursor = connection.cursor()
        for result in data['results']:
            dt = datetime.fromtimestamp(result['t'] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(f'''
                INSERT INTO {table} (timestamp, datetime, ticker, open, high, low, close, volume, volume_weighted, trades)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                result['n']
            ))
        connection.commit()
        connection.close()
        return data
    else:
        return f"Error: {response.status_code}"
