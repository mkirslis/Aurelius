from datetime import datetime
from logger import Logger
from database_manager import DatabaseManager
from strategy_manager import StrategyManager


def main():
    # Start
    start_time = datetime.now()

    # Configuration
    PROJECT_NAME = "Aurelius"
    VERSION_NUMBER = "0.0.6"
    START_DATE = "2024-01-01"
    END_DATE = "2024-12-31"
    DATABASES = ['stocks.db']
    TABLES = ['ohlcv_daily', 'ohlcv_5min']
    TICKERS = ['JPM', 'GS']

    # Initialize
    logger = Logger(PROJECT_NAME, VERSION_NUMBER)
    logger.output(f"{PROJECT_NAME} v{VERSION_NUMBER} started.")
    database_manager = DatabaseManager(DATABASES, TABLES, TICKERS, logger)
    
    # Execution
    database_manager.initialize_database()
    database_manager.procure_data(START_DATE, END_DATE)
    database_tables = database_manager.get_database_tables()

    strategy_manager = StrategyManager(database_tables, logger)
    strategy_manager.plot_histograms()

    # End
    runtime = datetime.now() - start_time
    logger.output(f"{PROJECT_NAME} completed. Runtime: {runtime}")


if __name__ == "__main__":
    main()
