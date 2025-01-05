from config import *
from datetime import datetime, timedelta
import sqlite3
import os
import logging

logging.basicConfig(filename="app.log",
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def initialize(databases):  # Creates databases & tables or verifies their existence
    logging.info("Initializing databases...")

    for database in databases:
        try:
            if os.path.exists(database):
                logging.info(f"Database '{database}' already exists.")
            else:
                try:
                    with sqlite3.connect(database) as connection:
                        logging.info(f"Database '{database}' created.")
                except sqlite3.Error as e:
                    logging.error(f"Database error: {e}")
                    continue
            try:
                with sqlite3.connect(database) as connection:
                    cursor = connection.cursor()
                    connection.execute("BEGIN")
                    if database in DATABASES:
                        for table, query in DATABASES[database]["tables"].items():
                            cursor.execute(
                                f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table}'")
                            if cursor.fetchone()[0] == 0:
                                cursor.execute(query)
                                logging.info(
                                    f"Table '{table}' created in database '{database}'.")
                            else:
                                logging.info(
                                    f"Table '{table}' already exists in database '{database}'.")
                        connection.commit()
            except sqlite3.Error as e:
                connection.rollback()
                logging.error(f"Error creating or verifying tables in database '{
                              database}': {e}")
            finally:
                cursor.close()
        except OSError as e:
            logging.error(f"File error: {e}")


def update(databases):
    current_market_day = '2025-01-03'
    logging.info(f"Latest market data is from {current_market_day}.")
    for database, items in databases.items():
        tickers = items.get("tickers", [])
        tables = items.get("tables", {})
        logging.info(f"Checking database: {database}...")
        if not os.path.exists(database):
            logging.warning(f"Database '{database}' does not exist. Skipping...")
            continue
        try:
            with sqlite3.connect(database) as connection:
                cursor = connection.cursor()
                for table in tables.keys():
                    for ticker in tickers:
                        try:
                            query = f"""
                            SELECT * FROM {table}
                            WHERE ticker = ? AND t = ?
                            """
                            cursor.execute(query, (ticker, current_market_day))
                            result = cursor.fetchall()
                            if result:
                                logging.info(f"{database}, {ticker}, {table} is up to date.")
                            else:
                                logging.info(f"{database}, {ticker}, {table} needs updating...")
                                # Update mechanism here
                        except sqlite3.Error as e:
                            logging.error(f"Database error in '{database}': {e}")
        except sqlite3.Error as e:
            logging.error(f"Database error in '{database}': {e}")
