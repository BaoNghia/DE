# Raw Package
import os
import numpy as np
import pandas as pd
# Data Source
import yfinance as yf
from datetime import datetime, date, timedelta


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
    tickers = ["AAPL", "GOOG", "TSLA","FB",
                "NFLX","AMZN","JNJ","GGPI",
                "SHOP","AOS","BK"]
    start ="2020-01-01"
    today = date.today()
    yesterday = today - timedelta(days=1)
    crawl_stock(tickers, start, yesterday)

    # print(yf.Ticker(tickers).history(start ="2021-01-01", end="2021-10-31" , interval='1h'))