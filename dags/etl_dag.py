from airflow import DAG

try:
    from datetime import datetime
    from datetime import timedelta
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    from airflow.operators.python import PythonOperator
    from source_database import start_reference_db_ingestion
    from aws_datalake import initialize_data_lake
    from aws_datalake import extract_source
    from aws_datalake import transform_load_s3
    from aws_datalake import generate_s3_prefix
    from aws_datalake import load_s3_to_maria

    # import os
    print("All Modules Imported Successfully")
except Exception as e:
    print("Error {} importing module".format(e))

"""
DAG definition
"""
default_args = {
    'owner': 'airflow',
    'retries': 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 3, 30)
}


def start_etl():
    print(" ============== The ETL Process Started at {} ================".format(datetime.now().__str__()))


def postgres_to_s3():
    print(" \n\n ==== Starting Postgres To S3 Data Lake Extraction.  ======== \n\n")
    data_lake = initialize_data_lake()
    dataframe = extract_source()
    transform_load_s3(data_lake, dataframe)


def s3_to_maria_db():
    print(" \n\n ==== Starting S3 Date Lake to Maria DB Transformation & Loading.  ======== \n\n")
    s3_bucket_dir_prefix = generate_s3_prefix()
    load_s3_to_maria(s3_bucket_dir_prefix)


def end_etl():
    print(" ============== The ETL Process Ended at {} ================".format(datetime.now().__str__()))


with DAG(
        dag_id='Medical_Insurance_Airflow_ETL',
        schedule='@daily',
        default_args=default_args,
        catchup=False
) as dg:
    # Uncomment and define functions if needed
    start = PythonOperator(
        task_id='Start_ETL',
        python_callable=start_etl,  # Replace with actual function
        provide_context=True
    )

    ingest_to_postgres = PythonOperator(
        task_id='Ingest_To_Postgres',
        python_callable=start_reference_db_ingestion
    )

    extract_to_s3 = PythonOperator(
        task_id='Extract_To_S3',
        python_callable=postgres_to_s3
    )

    load_to_db = PythonOperator(
        task_id='Transform_Load_To_MariaDb',
        python_callable=s3_to_maria_db
    )

    end = PythonOperator(
        task_id='End_ETL',
        python_callable=end_etl
    )

    start >> ingest_to_postgres >> extract_to_s3 >> load_to_db >> end
