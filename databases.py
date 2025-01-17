OHLCV_DAILY = '''
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
    trades INTEGER )
'''

BASELINE_TIMESTAMPS = '''
CREATE TABLE IF NOT EXISTS baseline_timestamps (
    timestamp INTEGER )
'''

DATABASES = {
    "Stocks.db": {
        "tickers": ["JPM", "BAC", "C", "WFC", "GS", "MS"],
        "tables": {
            "ohlcv_daily": OHLCV_DAILY
        }
    },

    "BaselineTimestamps.db": {
        "tickers": ["SPY"],
        "tables": {
            "baseline_timestamps": BASELINE_TIMESTAMPS
        }
    },
}
