from config import *
from databases import *
from datetime import datetime, timedelta, time
import sqlite3
import os
import pandas as pd
import csv
import pytz
import logging

PROJECT_NAME = "Aurelius"
VERSION_NUMBER = "0.0.3"

EPOCH_MARKET_DATE = "2023-01-17"
LATEST_MARKET_DATE = "2025-01-10"


logging.basicConfig(filename="app.log",
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def output(message, log_level="info"):
    """
    Print a message to the console and log it simultaneously.
    
    Args:
        message (str): The message to print and log.
        log_level (str): The logging level as a string (default is "info").
                         Valid levels: "debug", "info", "warning", "error", "critical".

    # Logging Levels:
    # Level      String       Numeric Value    Description
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
    """Check if the database file exists."""
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
    """Create the database (used if the dt does not exist)."""
    try:
        with sqlite3.connect(database):
            output(f"Database '{database}' created.")
    except sqlite3.Error as e:
        output(f"Error creating database '{database}': {e}", "error")


def check_table_exists(connection: sqlite3.Connection, database: str, table_name: str) -> bool:
    """Check if a table exists in the database."""
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
    """Create a table in the database if it does not exist."""
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        output(f"Table '{database}, {table_name}' created.")
    except sqlite3.Error as e:
        output(f"Error creating table '{database}, {table_name}': {e}", "error")
    finally:
        cursor.close()


def read_csv_col(csv, col): 
    """Reads in a column of dates from a CSV file (to establish market dates)."""
    df = pd.read_csv(csv)
    output(f"Loaded column '{col}' from CSV.")
    return df[col]


def convert_dates_to_unix(dates):
    """Converts datetimes @ 00:00 EST to Unix timestamps."""
    est = pytz.timezone('US/Eastern')
    unix_timestamps = []
    for date in dates:
        if pd.isna(date): continue
        dt = datetime.strptime(date, '%m/%d/%Y')
        dt = est.localize(dt)
        unix_timestamp = int(dt.timestamp() * 1000)
        unix_timestamps.append(unix_timestamp)
    return unix_timestamps