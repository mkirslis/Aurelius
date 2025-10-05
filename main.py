from datetime import datetime
from logger import Logger
from database_manager import DatabaseManager
from strategy_manager import StrategyManager


def main():

    ### INTRO
    start_time = datetime.now()
    PROJECT_NAME = "Aurelius"
    VERSION_NUMBER = "0.0.7"
    START_DATE = "2025-09-22" 
    END_DATE = "2025-09-26"
    DATABASES = ['database.db']
    TABLES = ['ohlcv_daily', 'market_caps_daily']
    TICKERS = ['JPM', 'GS', 'WFC', 'MS', 'C', 'BAC']

    ### LOGGER
    logger = Logger(PROJECT_NAME, VERSION_NUMBER)
    logger.output(f"{PROJECT_NAME} v{VERSION_NUMBER} started.")
    
    ### DATABASE
    database_manager = DatabaseManager(DATABASES, TABLES, TICKERS, logger)
    database_manager.initialize_database()
    unique_dates = database_manager.get_unique_dates(START_DATE, END_DATE)
    database_manager.procure_data(START_DATE, END_DATE, unique_dates)
    database_manager.join_tables()
    database_tables = database_manager.get_database_tables()

    ### STRATEGY
    strategy_manager = StrategyManager(database_tables, logger)
    strategy_manager.check_for_duplicates()
    strategy_manager.summarize()
    strategy_manager.create_strategies()
    # strategy_manager.plot_histograms()

    ### END
    runtime = datetime.now() - start_time
    logger.output(f"{PROJECT_NAME} completed. Runtime: {runtime}")


if __name__ == "__main__":
    main()