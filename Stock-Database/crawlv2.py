# Raw Package
import os
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import yfinance as yf
## Data viz
# import plotly.graph_objs as go


def crawl_stock(tickers, start, end, origin_path = "./data/bars"):
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(start=start, end=end, interval="1h")
            df.reset_index(level=0, inplace=True)
            df['Symbol'] = ticker
            # rename columns
            df.rename(columns={"index": "Time"}, inplace=True)
            df.columns= df.columns.str.lower()
            df.columns = df.columns.str.replace(' ', '_')
            df.to_csv(f"{origin_path}/{ticker}.csv", index = False)
            print(f"successfully {ticker}")
        except:
            pass

if __name__ == "__main__":
    tickers = ["AAPL", "GOOG", "TSLA"]
    start = datetime(2021,1,1)
    today = date.today()
    start = today - timedelta(days=4)
    end = today - timedelta(days=3)
    crawl_stock(tickers, start=start, end=end)
