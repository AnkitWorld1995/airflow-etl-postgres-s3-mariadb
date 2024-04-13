from helper.db_connections import Postgresql
from helper.db_connections import format_table_columns
import os
import pandas as pd

# CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def get_source_db_connection():
    PGX_HOST = os.getenv('PG_HOST')
    PGX_PORT = os.getenv('PG_PORT')
    PGX_USER_NAME = os.getenv('PG_USER')
    PGX_PASSWORD = os.getenv('PG_PASSWORD')
    PGX_DATABASE_NAME = os.getenv('PG_DATABASE')

    print("\n Source DB Connection ==>",PGX_HOST, PGX_PORT, PGX_USER_NAME, PGX_PASSWORD)

    pg_database = Postgresql(host=PGX_HOST,
                             port=PGX_PORT,
                             db_name=PGX_DATABASE_NAME,
                             user_name=PGX_USER_NAME,
                             password=PGX_PASSWORD
                             )
    return pg_database


def start_reference_db_ingestion():

    pg_database = get_source_db_connection()

    """
    Initialize PostgresSQL 
               database Schema
               database table
    """
    pg_database.drop_table('medical_insurance', 'insurance')
    pg_database.drop_schema('medical_insurance')
    pg_database.create_schema('medical_insurance')

    """
    Retrieve all tables Columns from CSV and Format accordingly.
    """
    medical_insurance_dataframe = pd.read_csv('/opt/airflow/dags/data/medical_insurance.csv')

    table_columns, columns = format_table_columns(df=medical_insurance_dataframe)
    pg_database.create_table('medical_insurance', 'insurance', table_columns)
    pg_database.truncate_table('medical_insurance', 'insurance')

    pg_database.insert_data(table_name='insurance', table_schema='medical_insurance', columns=columns,
                            df=medical_insurance_dataframe)

    pg_database.close_connection()
