from config import POLYGON_IO_API_KEY
from databases import DATABASES
from main import *


def main():
    """Start"""
    start = datetime.now()
    formatted_start = start.strftime("%Y-%m-%d %H:%M:%S")
    output(f"{PROJECT_NAME} v{VERSION_NUMBER}")
    output(f"Started: {formatted_start}")

    """Data Initialization"""
    for database, items in DATABASES.items():
        if not check_database_exists(database):
            create_database(database)

        with sqlite3.connect(database) as connection:
            for table, query in items["tables"].items():
                if not check_table_exists(connection, database, table):
                    create_table(connection, database, table, query)

    """Data Procurement"""
    aggregate_bars_baseline(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{EARLIEST_MARKET_DATE}/{LATEST_MARKET_DATE}?apiKey={POLYGON_IO_API_KEY}")

    # Check to see if all baseline timestamps * ticker * table combos exist
    # For those that do not, pull data
    # For any data with a timestamp older than the earliest date, assume its valid & exclude from checks
    # Remove duplicates in tables

    # Make sure all tickers have equal number of candles to baseline

    for database, items in DATABASES.items():
        if database != "BaselineTimestamps.db":
            tickers = items["tickers"]
            for table in items["tables"]:
                for ticker in tickers:
                    aggregate_bars(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{EARLIEST_MARKET_DATE}/{LATEST_MARKET_DATE}?apiKey={POLYGON_IO_API_KEY}",database,table)
                    output(f"Data pulled for {database}, {ticker}, {table}")


    """Data Validation"""
    for database, items in DATABASES.items():
        if database != "BaselineTimestamps.db":

            for table_name in items['tables']:
                for ticker in items['tickers']:
                    print(f"Processing ticker: {ticker} in table: {table_name}")

            connection.commit()
            connection.close()


    """End"""
    end_time = datetime.now()
    runtime = end_time - start
    formatted_end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    output(f"Ended: {formatted_end_time}")
    output(f"Runtime: {runtime}")


if __name__ == "__main__":
    main()  
