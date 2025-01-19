import sqlite3
import requests
import time
from datetime import datetime, timezone
from config import API_KEY
import pandas as pd

class DatabaseManager:
    def __init__(self, databases, tables, tickers, logger):
        self.databases = databases
        self.tables = tables
        self.tickers = tickers
        self.logger = logger

    def fetch_data(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.output(f"Failed to fetch data from {url}: {e}", "error")
            raise

    def aggregate_bars(self, connection, data, table):
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

    def initialize_database(self):
        schema = '''
            id INTEGER PRIMARY KEY,
            timestamp INTEGER,
            datetime TEXT,
            ticker TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            volume_weighted REAL,
            trades INTEGER,
            resultsCount INTEGER,
            request_id TEXT,
            UNIQUE (timestamp, datetime, ticker, open, high, low, close, volume, volume_weighted, trades)
        '''
        for database in self.databases:
            connection = sqlite3.connect(database)
            cursor = connection.cursor()
            for table in self.tables:
                self.logger.output(f"Initializing table {table} in {database}...")
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema})")
                self.logger.output(f"Table {table} in {database} is initialized.")
            connection.commit()
            connection.close()

    def procure_data(self, start_date, end_date):
        for database in self.databases:
            connection = sqlite3.connect(database)
            for table in self.tables:
                for ticker in self.tickers:
                    self.logger.output(f"Requesting data for {ticker} in {table} ({database})...")
                    time.sleep(15)  # Avoid API rate limits
                    interval = "1/day" if table == "ohlcv_daily" else "5/minute"
                    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{interval}/{start_date}/{end_date}?apiKey={API_KEY}"
                    data = self.fetch_data(url)
                    self.aggregate_bars(connection, data, table)
                    self.logger.output(f"Data stored for {ticker} in {table} ({database}).")
            connection.close()

    def get_database_tables(self):
        database_tables = {}
        for database in self.databases:
            connection = sqlite3.connect(database)
            for table in self.tables:
                dataframe = f"{database.split('.')[0]}_{table}"
                query = f"SELECT * FROM {table}"
                database_tables[dataframe] = pd.read_sql(query, connection)
            connection.close()
        
        return database_tables
