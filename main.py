import mysql.connector
from mysql.connector import Error  # connects to MySQL Workbench
import pandas as pd  # Data Transformation
import requests  # Data Extraction
from sqlalchemy import create_engine


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
        )
        print("MySQL Server connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


connection = create_server_connection("localhost", "root", "password")


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


create_database_query = "CREATE DATABASE if not exists MichiganSchools"
create_database(connection, create_database_query)


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


connection = create_db_connection("localhost", "root", "Comber00.", 'MichiganSchools')


def extract() -> dict:
    api_url = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(api_url).json()
    return data


def transform(data: dict) -> pd.DataFrame:
    # transforms dataset into desired structures and filters
    df = pd.DataFrame(data)
    print(f"Total Number of Universities from API {len(data)}")
    df = df[df["name"].str.contains("Michigan")]
    print(f"Number of universities in Michigan {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains", "country", "web_pages", "name"]]


def load(df: pd.DataFrame) -> None:
    # loads data into mysql database
    engine = create_engine("mysql+mysqlconnector://root:Comber00.@localhost/michiganschools")
    df.to_sql('mi_uni', engine, if_exists='replace')


try:
    data = extract()
    df = transform(data)
    load(df)

except Error as err:
    print(f"Error while extracting data: '{err}'")

