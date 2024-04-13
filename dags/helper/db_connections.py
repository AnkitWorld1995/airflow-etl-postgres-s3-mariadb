import os
import pandas as pd
import numpy as np
import psycopg2
from mysql import connector
from pandas import DataFrame
from datetime import datetime

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def format_table_columns(df: DataFrame):
    df['smoker'] = df['smoker'].astype(bool)
    col_dict = dict()
    for col_name, col_type in df.dtypes.to_dict().items():

        if col_type == np.float64:
            col_type = 'float8'

        elif col_type == np.int64:
            col_type = 'int8'

        elif col_type == np.bool_:
            col_type = 'BOOLEAN'

        else:
            col_type = 'VARCHAR(255)'
        col_dict[col_name] = col_type

    insert_column = ''
    for idx, col in enumerate(df.keys()):
        insert_column += f"{col}"

        if idx < len(df.keys()) - 1:
            comma = ', '
            insert_column += comma

    print(f"\n The Column Dict is: {col_dict} \n The Insert Columns are: {insert_column}")
    return col_dict, insert_column


class DbConnections:
    def __init__(self, host: str, port: str, db_name: str, user: str, password: str, database_type: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.database_type = database_type
        self.conn = self.establish_connection()
        self.cursor = self.conn.cursor()

    def establish_connection(self):
        """
        Establish a connection to the Local Database
        """
        try:
            if self.database_type == "postgresql":
                conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.db_name, user=self.user,
                                        password=self.password)
                conn.set_session(autocommit=True)
                print(f'Successfully connected to {self.db_name}!')

                return conn

            elif self.database_type == "mariasql":
                conn = connector.connect(host=self.host,
                                         port=self.port,
                                         database=self.db_name,
                                         user=self.user,
                                         password=self.password)
                conn.autocommit = True
                print(f'Successfully connected to {self.db_name}!')
                return conn

        except:
            print(
                f'Error when connecting to {self.database_type} database! Please check your docker container and make '
                f'sure database is running and you have given the correct informations!')

    def execute_query(self, query: str, return_data=True):

        try:
            self.cursor.execute(query)
            print('\n == Query Executed Successfully!')
            if return_data:
                columns = [desc[0] for desc in self.cursor.description]
                df = pd.DataFrame(self.cursor.fetchall(), columns=columns)
                return df

        except Exception as e:
            self.cursor.execute("ROLLBACK")
            self.close_connection()
            print(f"Error when executing query: '{e}'")
            raise Exception("There is a problem with your query. Please control it. Marking task as failed! ")

    def truncate_table(self, table_schema: str, table_name: str) -> None:
        query = f"TRUNCATE TABLE {table_schema}.{table_name}"
        print(query)
        self.execute_query(query, return_data=False)

    def create_schema(self, schema_name: str) -> None:
        query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        print(query)
        self.execute_query(query, return_data=False)

    def drop_schema(self, schema_name: str) -> None:
        query = f"DROP SCHEMA IF EXISTS {schema_name}"
        print(query)
        self.execute_query(query, return_data=False)

    def create_table(self, table_schema: str, table_name: str, columns: dict) -> None:
        query = f"CREATE TABLE IF NOT EXISTS {table_schema}.{table_name} ("
        modified_dict = [f"{column_name} {column_datatype}" for column_name, column_datatype in columns.items()]
        column_information = ",\n".join(modified_dict)
        query += column_information
        query += ');'
        self.execute_query(query, return_data=False)
        print(query)

    def drop_table(self, schema_name, table_name: str) -> None:
        query = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
        print(query)
        self.execute_query(query, return_data=False)

    def close_connection(self):
        """
        Terminates connection to the database
        """
        self.conn.close()
        self.cursor.close()
        print("Connection is successfully terminated!")

    def insert_data(self, table_name: str, table_schema: str, columns: str, df: DataFrame) -> None:
        """
        Inserts value to database for specified schema and table in the ETL pipeline.
        """

        sql_query = f"""INSERT INTO {table_schema}.{table_name} ({columns}) \n VALUES \n """

        # df.reset_index(drop=True, inplace=False)
        index = 0
        for _, rows in df.iterrows():
            insert_rows = list(rows)
            print(insert_rows)
            modified_insertion_values = "("

            for idx2, value in enumerate(insert_rows):
                if pd.isna(value):
                    insert_string = 'NULL'

                elif isinstance(value, pd.Timestamp):
                    insert_string = f"'{value}'"

                elif isinstance(value, str):
                    value = value.replace("'", "''")
                    insert_string = f"'{value}'"

                else:
                    insert_string = str(value)

                modified_insertion_values += insert_string
                if idx2 < len(insert_rows) - 1:
                    modified_insertion_values += ', '

            if index < len(df) - 1:
                modified_insertion_values += "),\n "
            else:
                modified_insertion_values += ");"

            sql_query += modified_insertion_values
            index = index + 1

        print('\n\n The Insert Query', sql_query, '\n\n And the Ingestion will start at', datetime.now().__str__())
        self.execute_query(sql_query, return_data=False)
        print(f'\n\nIngestion process has completed at {datetime.now().__str__()}!\n\n')


class Postgresql(DbConnections):
    def __init__(self, host: str, port: str, db_name: str, user_name: str, password: str):
        super().__init__(host, port, db_name, user_name, password, 'postgresql')


class MariaSql(DbConnections):
    def __init__(self, host: str, port: str, db_name: str, user_name: str, password: str):
        super().__init__(host, port, db_name, user_name, password, 'mariasql')
