import yaml
import psycopg2
from typing import Dict
from dataload import get_database
import pandas as pd
import sqlalchemy
from sqlalchemy import Column, Integer, String, Date, DateTime, Float, Integer, BigInteger
from sqlalchemy import Table
from sqlalchemy.ext.declarative import declarative_base 
from crawl import crawl_stock
from datetime import datetime, date, timedelta

def load_connection_info(ini_filename: str) -> Dict[str, str]:
    with open(ini_filename, 'r') as f:
        vals = yaml.safe_load(f)

    # Create a dictionary of the variables stored under the "postgresql" section of the .ini
    conn_info = vals
    return conn_info


def create_db(conn_info: Dict[str, str],) -> None:
    # Connect just to PostgreSQL with the user loaded from the .ini file
    psql_connection_string = f"user={conn_info['PGUSER']} password={conn_info['PGPASSWORD']}"
    conn = psycopg2.connect(psql_connection_string)
    cur = conn.cursor()

    # "CREATE DATABASE" requires automatic commits
    conn.autocommit = True
    sql_query = f"CREATE DATABASE {conn_info['PGDATABASE']}"

    try:
        cur.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")
        cur.close()
    else:
        # Revert autocommit settings
        conn.autocommit = False


Base = declarative_base()
class daily_prices(Base):
    __tablename__ = 'daily_prices'
    symbol = Column('symbol', String, primary_key=True)
    time = Column('time', DateTime, nullable=False, primary_key=True)
    open = Column('open', Float)
    high = Column('high', Float)
    low = Column('low', Float)
    close = Column('close', Float)
    volume = Column('volume', BigInteger)
    dividends = Column('dividends', Float)
    stock_splits = Column('stock_splits', Integer)


if __name__ == "__main__":
    config_file = "db.yaml"
    # host, database, user, password
    conn_info = load_connection_info(config_file)
    print(conn_info)
    # Create the desired database
    create_db(conn_info)
    engine = get_database(config_file)

    # Load bars into the database
    try:
        Base.metadata.create_all(engine)
        print("Daily prices table is created")
    except:
        print("Daily prices table is not created")

    # create first csv 
    tickers = ["AAPL", "GOOG", "TSLA"]
    start = datetime(2021,1,1)
    today = date.today()
    end = today - timedelta(days=4)
    crawl_stock(tickers, start=start, end=end)
