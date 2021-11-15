import pandas as pd
import psycopg2
import yaml
import os
from sqlalchemy import create_engine
from tqdm.notebook import tqdm
import glob


def get_database(config_file = "db.yaml"):
    try:
        engine = get_connection_from_profile(config_file)
        print("Connected to PostgreSQL database!")
    except IOError:
        print("Failed to get database connection!")
        return None, 'fail'
    return engine

def get_connection_from_profile(config_file_name="db.yaml"):
    """
    Sets up database connection from config file.
    Input:
    config_file_name: File containing PGHOST, PGUSER,
                        PGPASSWORD, PGDATABASE, PGPORT, which are the
                        credentials for the PostgreSQL database
    """
    with open(config_file_name, 'r') as f:
        vals = yaml.safe_load(f)
    if not ('PGHOST' in vals.keys() and
            'PGUSER' in vals.keys() and
            'PGPASSWORD' in vals.keys() and
            'PGDATABASE' in vals.keys()):
        raise Exception('Bad config file: ' + config_file_name)

    return get_engine(vals['PGDATABASE'], vals['PGUSER'],
                        vals['PGHOST'], vals['PGPASSWORD'])

def get_engine(db, user, host, passwd):
    """
    Get SQLalchemy engine using credentials.
    Input:
    db: database name
    user: Username
    host: Hostname of the database server
    port: Port number
    passwd: Password for the database
    """
    url = 'postgresql://{user}:{passwd}@{host}/{db}'.format(
        user=user, passwd=passwd, host=host, db=db)
    engine = create_engine(url, pool_size = 50, echo=True)
    return engine


bars_path = 'data/bars'
tickers_path = 'data/tickers'

# This function will build out the sql insert statement to upsert (update/insert) new rows into the existing pricing table
def import_bar_file(symbol_path, engine):
    # path = bars_path + '/{}.csv'.format(symbol)
    df = pd.read_csv(symbol_path, index_col=[0], parse_dates=[0])
    
    # Some clean up for missing data
    if 'dividends' not in df.columns:
        df['dividends'] = 0
    df = df.fillna(0.0)

    # First part of the insert statement
    insert_init = """INSERT INTO daily_prices (symbol,time,open,high,low,close,volume,dividends,stock_splits)
                    VALUES
                """

    # Add values for all days to the insert statement
    vals = ",".join(["""('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(
                        row.symbol,
                        time,
                        row.open,
                        row.high,
                        row.low,
                        row.close,
                        row.volume,
                        row.dividends,
                        row.stock_splits, 
                    ) for time, row in df.iterrows()])
    
    # Handle duplicate values - Avoiding errors if you've already got some data in your table
    insert_end = """ ON CONFLICT (symbol, time) DO UPDATE 
                SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                dividends = EXCLUDED.dividends,
                stock_splits = EXCLUDED.stock_splits;
                """

    # Put together the query string
    query = insert_init + vals + insert_end
    
    # Fire insert statement
    engine.execute(query)

# This function will loop through all the files in the directory and process the CSV files
def process_symbols(engine):
    symbols = glob.glob(f'{bars_path}/*.csv')
    # symbols = ['AAPL', 'MO']
    for symbol in tqdm(symbols, desc='Importing...'):
        import_bar_file(symbol, engine)

    return 'Process symbols complete'        


def process_tickers(engine):
    # Read in the tickers file from a csv
    df = pd.read_csv('{}/polygon_tickers_us.csv'.format(tickers_path))

    # Formatting
    df = df[['ticker', 'name', 'market', 'locale', 'type', 'currency', 'active', 'primaryExch', 'updated']]
    df.rename(columns={'ticker': 'symbol', 'name': 'symbol_name', 'primaryExch': 'primary_exch'}, inplace=True)
    df['updated'] = pd.to_datetime('now')

    # Run this once to create the table
    df.to_sql('tickers', engine, if_exists='replace', index=False)
    
    # Add a primary key to the symbol
    query = """ALTER TABLE tickers 
                ADD PRIMARY KEY (symbol);"""
    engine.execute(query)
    return 'Tickers table created'
                
                
if __name__ == "__main__":
    config_file = "db.yaml"
    engine = get_database(config_file)
    print(engine)
    # Load bars into the database
    process_symbols(engine)

    # Load tickers into the database    
    process_tickers(engine)
    # # Read in the PostgreSQL table into a dataframe
    # prices_df = pd.read_sql('daily_prices', engine, index_col=['symbol', 'date'])

    ## Show results of df
    ## I can also pass in a sql query
    # prices_df2 = pd.read_sql_query('select * from daily_prices', engine, index_col=['symbol', 'date'])

    # # Plot the results
    # prices_df2.loc[['TSLA']]['close_adj'].plot()
