import matplotlib.pyplot as plt
import os


class StrategyManager:
    def __init__(self, data: dict, logger):
        self.data = data
        self.logger = logger

    def plot_histograms(self):
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
