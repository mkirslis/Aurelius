import matplotlib.pyplot as plt
import pandas as pd
import os


class StrategyManager:
    def __init__(self, data: dict, logger):
        self.data = data
        self.logger = logger


    def check_for_duplicates(self):
        """
        Checks data for duplicate ticker x [interval] rows.
        """
        for df_name, df in self.data.items():
            df = df.groupby(['date', 'ticker']).size().unstack(fill_value=0)
            df = df[(df != 1).any(axis=1)] # Filter to only show rows where ticker x [interval] count > 1
            if df.empty:
                self.logger.output("No duplicated data by ticker x [interval].")
            else:
                self.logger.output("WARNING: Duplicate data detected - see terminal for df.")
                print(df)

        return(df)
    

    def summarize(self):
        """
        Gets summary & descriptive statistics for tickers from all tables. 
        """
        print(type(self.data.items()))
        for i, j in self.data.items():
            print(type(i))
            print(i)
            print(type(j))
            print(j)
    

    def create_strategies(self):
        for df_name, df in self.data.items():

            ### INTIAL STRATEGY
            if df_name == "database_ohlcv_market_caps_daily":

                # Develop initial market cap weighted %s for each ticker in the starting buy-and-hold portfolio
                initial_portfolio_value = 10000
                df = df.sort_values(['ticker', 'date']) 
                initial_df = df.drop_duplicates(subset='ticker', keep='first').copy()
                initial_total_market_cap =  initial_df['market_cap'].sum()
                self.logger.output(f"Intial total market cap: {initial_total_market_cap}")
                initial_df['market_cap_weight'] = initial_df['market_cap'] / initial_total_market_cap
                initial_weights = initial_df[['ticker', 'market_cap_weight']].rename(columns={'market_cap_weight': 'weight'})

                # Purchase shares for backtest
                first_prices = initial_df.set_index('ticker')['close']
                shares = (initial_weights.set_index('ticker')['weight'] * initial_portfolio_value) / first_prices
                df = df.merge(shares.rename('shares'), left_on='ticker', right_index=True)

                # Caclulate daily performance
                df['position_value'] = df['close'] * df['shares']
                daily_portfolio = df.groupby('date')['position_value'].sum().reset_index()
                daily_portfolio.rename(columns={'position_value': 'portfolio_value'}, inplace=True)
                daily_portfolio['daily_return'] = (daily_portfolio['portfolio_value'].pct_change() * 100).map("{:.2f}%".format)
                daily_portfolio['cum_return'] = ((daily_portfolio['portfolio_value'] / initial_portfolio_value - 1) * 100).map("{:.2f}%".format)

                print(df)
                print(daily_portfolio)

            ### OTHER STRATEGIES

            else:
                self.logger.output(f"No available strategies for {df_name}.")

        return(0)


    def plot_histograms(self):
        """
        WIP tools for visualization. 
        """
        if not os.path.exists('plots'):
            os.makedirs('plots')

        for df_name, df in self.data.items():
            numeric_columns = df.select_dtypes(include='number').columns

            if 'ticker' in df.columns:
                tickers = df['ticker'].unique()

                for ticker in tickers:
                    ticker_data = df[df['ticker'] == ticker]

                    for col in numeric_columns:
                        self.logger.output(f"Plotting {df_name}_{col}_{ticker}_hist...")
                        
                        plot_dir = f'plots/{df_name}/{ticker}'
                        if not os.path.exists(plot_dir):
                            os.makedirs(plot_dir)
                        
                        plt.figure(figsize=(16, 9))
                        plt.hist(ticker_data[col], bins=30, color='black', alpha=0.7, density=True)
                        plt.title(f'{col} in {df_name} for {ticker}')
                        plt.xlabel(col)
                        plt.ylabel('pct')
                        plt.tight_layout()
                        plt.savefig(f'{plot_dir}/{df_name}_{col}_{ticker}_hist.png')
                        self.logger.output(f"{df_name}_{col}_{ticker}_hist stored.")
                        plt.close()
    