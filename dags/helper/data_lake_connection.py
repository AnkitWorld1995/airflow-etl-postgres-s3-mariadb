import boto3
import os
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from uuid import uuid4
import pandas as pd


class DataLakeConnection:
    def __init__(self, storage_type: str, bucket_name: str, base_url: str, provider: str) -> None:
        self.storage_type = str(storage_type)
        self.bucket_name = bucket_name
        self.client = self.get_client()
        self.base_url = base_url.replace('BUCKET_NAME', self.bucket_name)
        self.provider = provider

    def get_client(self):
        s3_client = boto3.client(self.storage_type,
                                 aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                 aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                 region_name=os.getenv('AWS_REGION'))
        return s3_client

    def get_bucket(self):
        response = self.get_client().list_buckets()
        is_present = False
        for bucket in response['Buckets']:
            if bucket['Name'] == self.bucket_name:
                is_present = True

        if is_present:
            return self.bucket_name, is_present
        else:
            return "", is_present

    def check_bucket(self):
        bucket_name, is_valid = self.get_bucket()
        if not is_valid:
            raise Exception(f"The bucket {self.bucket_name} doesn't exists!.")
        else:
            return bucket_name

    def get_s3_object(self, prefix: str) -> []:

        listed_objects = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/')

        if 'Contents' not in listed_objects:
            print(f" \n\n The Content is not present in the Bucket {self.bucket_name} \n\n")
            return []

        file_list = []
        for obj in listed_objects['Contents']:
            file_list.append(obj['Key'])

        return file_list

    def transform_s3_object(self, file_object_list: []) -> DataFrame:
        combined_df = pd.DataFrame()
        bucket_url = f'https://{self.bucket_name}.{self.storage_type}.{self.provider}'

        for obj in file_object_list:
            file = "/".join([bucket_url, obj])

            df = pd.read_parquet(file, engine='pyarrow')
            combined_df = pd.concat([combined_df, df], ignore_index=False, sort=True)

        return combined_df

    def save_file_parquet(self, dframe: DataFrame):
        """
        Save file as Parquet Format in-memory
        Upload a file to an S3 bucket
        """

        dt = datetime.datetime.now()
        year = dt.year
        month = dt.month
        day = dt.day
        current_date = datetime.date.today().strftime('%d-%b-%Y')
        file_name = "".join(
            [f'medical_insurance/year_{year}/month_{month}/day_{day}/insurance_{current_date}_{uuid4()}.parquet'])

        """
        Save the file to A Table in Memory.
        """
        table = pa.Table.from_pandas(df=dframe)
        parquet_byte = pa.BufferOutputStream()
        pq.write_table(table, parquet_byte, store_schema=True)

        try:
            self.client.put_object(Body=parquet_byte.getvalue().to_pybytes(), Bucket=self.bucket_name, Key=file_name)

        except Exception as e:
            print("Error uploading File to S3".format(e))

            return {
                'Status': 400,
                'Message': 'Bad request!'
            }

        return {
            'Status': 200,
            'Message': 'File uploaded to S3 Successfully!'
        }


class AwsS3(DataLakeConnection):
    def __init__(self, storage_type: str, bucket_name: str, base_url: str, provider: str) -> None:
        super().__init__(storage_type, bucket_name, base_url, provider)
