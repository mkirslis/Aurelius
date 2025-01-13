DATABASES = {
    "Stocks.db": {
        "tickers": ["AAPL", "MSFT", "AMZN", "GOOGL", "META"],
        "tables": {
            "ohlcv_daily": '''
                CREATE TABLE IF NOT EXISTS ohlcv_daily (
                    request_id TEXT, 
                    queryCount INTEGER, 
                    resultsCount INTEGER, 
                    status TEXT, 
                    adjusted BOOLEAN, 
                    t INTEGER, 
                    ticker TEXT, 
                    o REAL, 
                    h REAL, 
                    l REAL, 
                    c REAL, 
                    v INTEGER, 
                    vw REAL,
                    n INTEGER )
                '''
        }
    },
    "Forex.db": {
        "tickers": ["C:EURUSD", "C:JPYUSD", "C:GBPUSD", "C:AUDUSD", "C:CADUSD", "C:CHFUSD", "C:NZDUSD"],
        "tables": {
            "ohlcv_daily": '''
                CREATE TABLE IF NOT EXISTS ohlcv_daily (
                    request_id TEXT, 
                    queryCount INTEGER, 
                    resultsCount INTEGER, 
                    status TEXT, 
                    adjusted BOOLEAN, 
                    t INTEGER, 
                    ticker TEXT, 
                    o REAL, 
                    h REAL, 
                    l REAL, 
                    c REAL, 
                    v INTEGER, 
                    vw REAL,
                    n INTEGER )
                '''
        }
    }
}