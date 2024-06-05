import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import sys
import urllib.parse

# Postgres connection details
server = os.getenv("DBT_SERVER_DEV")
username = os.getenv("DBT_USER_DEV")
password = os.getenv("DBT_PASSWORD_DEV")
database = os.getenv("DBT_DATABASE_DEV")
port = os.getenv("DBT_PORT_DEV")



def process_file(csv_file_path):
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path, encoding='latin-1')

    df.rename(columns=lambda x: x.replace('(', '_').replace(')', '_'), inplace=True)

    encoded_password = urllib.parse.quote_plus(password)

    # Constructing  the Postgres connection string using environment variables
    conn_str = (
                f'postgresql+psycopg2://{username}:{encoded_password}@{server}:{port}/{database}'
    )

    # Creating the SQLAlchemy engine
    engine = sqlalchemy.create_engine(conn_str)

    # Exporting the DataFrame to Postgres
    df.to_sql(name=table_name, con=engine, schema=schema_name, index=False, if_exists='replace')

    # Printing confirmation message indicating the table is updated
    print(f'Table "{table_name}" has been successfully updated in the database.')


if __name__ == "__main__":
    # Check if a file path is provided as an argument
    if len(sys.argv) != 4:
        print('Please check if you have supplied the path to the file, schema name & table name')
    else:
        file_path = sys.argv[1]
        schema_name = sys.argv[2]
        table_name = sys.argv[3]

        process_file(file_path)