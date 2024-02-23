import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import sys
import urllib.parse

# SQL Server connection details
server = os.getenv("DBT_SERVER")
username = os.getenv("DBT_USER")
password = os.getenv("DBT_PASSWORD")
port = os.getenv("DBT_PORT")

# CSV file path
#csv_file_path = 'C:/Users/Nobert Ngungu/Downloads/source_data/Cholera.csv'

def process_file(csv_file_path):
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path, encoding='latin-1')

    df.rename(columns=lambda x: x.replace('(', '_').replace(')', '_'), inplace=True)

    encoded_password = urllib.parse.quote_plus(password)

    # Constructing  the SQL Server connection string using environment variables
    conn_str = (
                f'mssql+pyodbc://{username}:{encoded_password}@{server}, {port}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
    )

    # Creating the SQLAlchemy engine
    engine = sqlalchemy.create_engine(conn_str)

    # Exporting the DataFrame to SQL Server
    df.to_sql(name=table_name, con=engine, schema='dbo', index=False, if_exists='replace')

    # Printing confirmation message indicating the table is updated
    print(f'Table "{table_name}" has been successfully updated in the database.')


if __name__ == "__main__":
    # Check if a file path is provided as an argument
    if len(sys.argv) != 4:
        print('Please check if you have supplied the path to the file, database name & table name')
    else:
        file_path = sys.argv[1]
        database = sys.argv[2]
        table_name = sys.argv[3]

        process_file(file_path)