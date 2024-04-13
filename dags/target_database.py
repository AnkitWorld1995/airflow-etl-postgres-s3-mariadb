import os
from helper.db_connections import MariaSql, format_table_columns
from pandas import DataFrame


def get_target_database_connection():
    maria_host = os.getenv("MARIA_HOST")
    maria_port = os.getenv("MARIA_PORT")
    maria_user_name = os.getenv("MARIA_USER")
    maria_password = os.getenv("MARIA_PASSWORD")
    maria_db = os.getenv("MARIA_DB")

    print(maria_host, maria_port, maria_user_name, maria_password, maria_db)

    maria_database = MariaSql(host=maria_host,
                              port=maria_port,
                              db_name=maria_db,
                              user_name=maria_user_name,
                              password=maria_password
                              )
    return maria_database


def start_target_db_ingestion(medical_insurance_dataframe: DataFrame):
    maria_db = get_target_database_connection()

    maria_db.create_schema('medical_insurance')

    table_columns, columns = format_table_columns(df=medical_insurance_dataframe)
    maria_db.create_table('medical_insurance', 'insurance', columns=table_columns)

    maria_db.insert_data(table_name='insurance', table_schema='medical_insurance', columns=columns,
                         df=medical_insurance_dataframe)

    maria_db.close_connection()
