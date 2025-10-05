import sqlite3
import requests
import time
from datetime import datetime, timezone, timedelta
from config import API_KEY
import pandas as pd

class DatabaseManager:
    def __init__(self, databases, tables, tickers, logger):
        self.databases = databases
        self.tables = tables
        self.tickers = tickers
        self.logger = logger


    def get_data(self, url):
        """ 
        Helper method that facilitates executing GET requests. 
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.output(f"Failed to get data from {url}: {e}", "error")
            raise


    def get_unique_dates(self, start_date, end_date):
        """ 
        Gets list of all dates in YYYY-MM-DD format between the global variables START_DATE & END_DATE. 
        """
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        delta = (end_date - start_date).days
        unique_dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta + 1)]

        return unique_dates
    

    def initialize_database(self):
        """
        Creates SQL databases & tables according to the schemas defined within the method.
        """

        ###################
        ### SQL SCHEMAS ### 
        ###################

        ### OHLCV DAILY TABLE
        ohlcv_daily_schema = '''
            id INTEGER PRIMARY KEY,
            timestamp INTEGER,
            datetime TEXT,
            date DATE,
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
            etl_datetime TEXT,
            UNIQUE (timestamp, datetime, ticker, open, high, low, close, volume, volume_weighted, trades)
        '''

        ### MARKET CAPS DAILY TABLE
        market_caps_daily_schema = '''
            id INTEGER PRIMARY KEY,
            date TEXT,
            ticker TEXT,
            share_class_shares_outstanding INTEGER,
            weighted_shares_outstanding INTEGER,
            market_cap INTEGER,
            request_id TEXT,
            etl_datetime TEXT,
            UNIQUE (date, ticker,  share_class_shares_outstanding, weighted_shares_outstanding, market_cap)
        '''

        for database in self.databases:
            # Create SQL connection & cursor object
            connection = sqlite3.connect(database)
            cursor = connection.cursor()

            #####################
            ### SQL EXECUTION ### 
            #####################

            for table in self.tables:

                ### OHLCV DAILY TABLE
                if table == "ohlcv_daily":
                    self.logger.output(f"Initializing table {table} in {database}...")
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({ohlcv_daily_schema})")
                    self.logger.output(f"Table {table} in {database} is initialized.")

                ### MARKET CAPS DAILY TABLE
                elif table == "market_caps_daily":
                    self.logger.output(f"Initializing table {table} in {database}...")
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({market_caps_daily_schema})")
                    self.logger.output(f"Table {table} in {database} is initialized.")
                
                ### ERROR HANDLING
                else:
                    self.logger.output(f"Unknown table '{table}' encountered in initialize_database()") # Handles unknown table names

            # Commit SQL query & close connection
            connection.commit()
            cursor.close()
            connection.close()


    def procure_data(self, start_date, end_date, unique_dates):
        """
        Performs GET requests to get data from polygon.io.
        """
        for database in self.databases:
            connection = sqlite3.connect(database)
            for table in self.tables:

                ### OHLCV DAILY TABLE ###
                if table == "ohlcv_daily":
                    for ticker in self.tickers:

                        # GET request...
                        self.logger.output(f"Requesting OHLCV data for {ticker} in {table} ({database})...") # Log API request
                        time.sleep(12.5)  # Avoid API rate limits
                        interval = "1/day"
                        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{interval}/{start_date}/{end_date}?apiKey={API_KEY}"
                        data = self.get_data(url)

                        # Data processing...
                        if data['results']:
                            for result in data['results']:
                                try:
                                    dt = datetime.fromtimestamp(result['t'] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                                    date = datetime.fromtimestamp(result['t'] / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
                                    cursor = connection.cursor()
                                    etl_datetime = datetime.now()
                                    cursor.execute(f'''
                                        INSERT OR IGNORE INTO {table} (timestamp, datetime, date, ticker, open, high, low, close, volume, volume_weighted, trades, resultsCount, request_id, etl_datetime)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ''', (
                                        result['t'],
                                        dt,
                                        date,
                                        data['ticker'],
                                        result['o'],
                                        result['h'],
                                        result['l'],
                                        result['c'],
                                        result['v'],
                                        result['vw'],
                                        result['n'],
                                        data['resultsCount'],
                                        data['request_id'],
                                        etl_datetime
                                    ))
                                    connection.commit()
                                    self.logger.output(f"OHLCV data stored for {date} | {ticker} in {table} ({database}).")
                                
                                except ValueError as ve:
                                    self.logger.output(f"Error: {ve}")  # Handle empty results gracefully
                                except KeyError as ke:
                                    self.logger.output(f"Missing key in data: {ke}")  # Handle missing keys in the data
                                except Exception as e:
                                    self.logger.output(f"An unexpected error occurred: {e}")  # Catch other unforeseen errors
                                    

                        # self.aggregate_bars(connection, data, table)
                        # self.logger.output(f"Data stored for {ticker} in {table} ({database}).")

                ### MARKET CAPS DAILY TABLE ###
                elif table == "market_caps_daily":
                    for ticker in self.tickers:
                        for date in unique_dates:

                            # GET request...
                            self.logger.output(f"Requesting market cap data for {date} | {ticker} in {table} ({database})...") # Log API request
                            time.sleep(12.5)  # Avoid API rate limits
                            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?date={date}&apiKey={API_KEY}"
                            data = self.get_data(url)

                            # Data processing...
                            if data['results']:
                                try:
                                    cursor = connection.cursor()
                                    etl_datetime = datetime.now()
                                    cursor.execute(f'''
                                        INSERT OR IGNORE INTO {table} (date, ticker, share_class_shares_outstanding, weighted_shares_outstanding, market_cap, request_id, etl_datetime)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)
                                        ''', (
                                            date,
                                            data['results']['ticker'],
                                            data['results']['share_class_shares_outstanding'],
                                            data['results']['weighted_shares_outstanding'],
                                            data['results']['market_cap'],
                                            data['request_id'],
                                            etl_datetime
                                        ))
                                    connection.commit()
                                    self.logger.output(f"Market cap data stored for {date} | {ticker} in {table} ({database}).")

                                except ValueError as ve:
                                    self.logger.output(f"Error: {ve}")  # Handle empty results gracefully
                                except KeyError as ke:
                                    self.logger.output(f"Missing key in data: {ke}")  # Handle missing keys in the data
                                except Exception as e:
                                    self.logger.output(f"An unexpected error occurred: {e}")  # Catch other unforeseen errors
                            else:
                                self.logger.output(f"*** No market cap data! ***   {date} | {ticker} in {table} ({database}).") # Handle missing data
                
                ### ERROR HANDLING
                else:
                    self.logger.output(f"Unknown table {table} encountered in procure_data() method...") # Handles incorrect table names

            # Close database SQL connection 
            cursor.close()
            connection.close()


    def join_tables(self):
        for database in self.databases:
            connection = sqlite3.connect(database)
            cursor = connection.cursor()
            cursor.execute("DROP TABLE IF EXISTS ohlcv_market_caps_daily;")
            cursor.execute("""
                CREATE TABLE ohlcv_market_caps_daily AS
                    SELECT o.*, m.market_cap
                    FROM ohlcv_daily AS o
                    JOIN market_caps_daily AS m ON (o.ticker = m.ticker AND o.date = m.date);
                """)
            connection.commit()
            cursor.close()
            connection.close()

        return 0


    def get_database_tables(self):
        """
        Returns a dictionary of all SQLite database tables as Pandas DataFrames. Automatically detects all table names in each database.
        """
        database_tables = {}

        for database in self.databases:
            conn = sqlite3.connect(database)
            cursor = conn.cursor()

            # Get all table names from sqlite_master
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = [row[0] for row in cursor.fetchall()]

            # Load each table into a DataFrame
            for table in table_names:
                key_name = f"{database.split('.')[0]}_{table}"
                query = f"SELECT * FROM {table}"
                database_tables[key_name] = pd.read_sql(query, conn)

            cursor.close()
            conn.close()

        return database_tables

    

