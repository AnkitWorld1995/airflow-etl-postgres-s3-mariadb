from helper.data_lake_connection import AwsS3
from pandas import DataFrame
from source_database import get_source_db_connection
from target_database import start_target_db_ingestion
import datetime
import os


def initialize_data_lake():
    """
    Load Env File
    Get Environment Variables and Keys
    """
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    BASE_S3_URL = os.getenv('BASE_S3_URL')
    PROVIDER = os.getenv('PROVIDER')
    print("\n\n The Bucket Name is {}".format(BUCKET_NAME), "and base URL is {}".format(BASE_S3_URL), "provider {}".format(PROVIDER))
    data_lake = AwsS3(storage_type='s3', bucket_name=BUCKET_NAME, base_url=BASE_S3_URL, provider=PROVIDER)

    return data_lake


def extract_source() -> DataFrame:
    """
    Get Source DB connection.
    Covert the source data into a DataFrame.
    """
    pg_database = get_source_db_connection()

    df = pg_database.execute_query("SELECT * FROM medical_insurance.insurance", return_data=True)
    pg_database.close_connection()
    return df


def transform_load_s3(data_lake: AwsS3, df: DataFrame) -> None:
    """
    Save Dataframe as  Parquet in-memory.
    Upload file to AWS S3 Data Lake.
    """
    data_lake.save_file_parquet(df)


def generate_s3_prefix() -> str:
    dt = datetime.datetime.now()
    year = dt.year
    month = dt.month
    day = dt.day

    prefix = f'medical_insurance/year_{year}/month_{month}/day_{day}/'

    return prefix


def load_s3_to_maria(prefix: str):
    data_lake = initialize_data_lake()
    file_list = data_lake.get_s3_object(prefix)
    df = data_lake.transform_s3_object(file_list)
    start_target_db_ingestion(df)
