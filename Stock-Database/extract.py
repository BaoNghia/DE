from configparser import ConfigParser
import psycopg2
import psycopg2.extras as psql_extras
import pandas as pd
import yaml
from typing import Dict, List

def load_connection_info(
    ini_filename: str
) -> Dict[str, str]:
    with open(ini_filename, 'r') as f:
        vals = yaml.safe_load(f)
    # Create a dictionary of the variables stored under the "postgresql" section of the .ini
    conn_info = vals
    return conn_info

def get_column_names(table: str, cur: psycopg2.extensions.cursor) -> List[str]:
    cursor.execute(
        f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}';")
    col_names = [result[0] for result in cursor.fetchall()]
    return col_names

def get_data_from_db(
    query: str,
    conn: psycopg2.extensions.connection,
    cur: psycopg2.extensions.cursor,
    df: pd.DataFrame,
    col_names: List[str]
) -> pd.DataFrame:
    try:
        cur.execute(query)
        while True:
            # Fetch the next 100 rows
            query_results = cur.fetchmany(100)
            # If an empty list is returned, then we've reached the end of the results
            if query_results == list():
                break

            # Create a list of dictionaries where each dictionary represents a single row
            results_mapped = [
                {col_names[i]: row[i] for i in range(len(col_names))}
                for row in query_results
            ]

            # Append the fetched rows to the DataFrame
            df = df.append(results_mapped, ignore_index=True)

        return df

    except Exception as error:
        print(f"{type(error).__name__}: {error}")
        print("Query:", cur.query)
        conn.rollback()

if __name__ == "__main__":
    conn_info = load_connection_info("db.ini")
    # Connect to the "houses" database
    connection = psycopg2.connect(**conn_info)
    cursor = connection.cursor()

    # These names must match the columns names returned by the SQL query
    col_names = get_column_names("daily_prices", cursor)
    # Create an empty DataFrame that will have the returned data
    house_df = pd.DataFrame(columns=col_names)
    query = "SELECT * from daily_prices;"
    house_df = get_data_from_db(query, connection, cursor, house_df, col_names)
    print(house_df)