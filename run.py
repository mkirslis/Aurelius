from datetime import datetime
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

    """Data Validation"""
    timestamps_dict = {}
    for col_name in pd.read_csv("market_dates.csv").columns:
        dates = read_csv_col("market_dates.csv", col_name)
        timestamps = convert_dates_to_unix(dates)
        timestamps_dict[col_name] = timestamps
        output(f"Converted column '{col_name}' from dates to timestamps.")
    output("Converted & stored all date columns as timestamps.")

    """Data Procurement"""
    aggregate_bars(f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2024-05-29/2024-06-29?apiKey={POLYGON_IO_API_KEY}", "Stocks", "ohlcv_daily")

    """End"""
    end_time = datetime.now()
    runtime = end_time - start
    formatted_end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    output(f"Ended: {formatted_end_time}")
    output(f"Runtime: {runtime}")


if __name__ == "__main__":
    main()  
