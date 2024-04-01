import logging
import shutil

from pathlib import Path
from typing import Union

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


_DEFAULT_MINIO_CONNECTION_ID = 'minio'


class MinIOConnectionManager:

    def __init__(self, connection_id: str = None):
        self.client = S3Hook(aws_conn_id=connection_id if connection_id else _DEFAULT_MINIO_CONNECTION_ID)

    def check_for_bucket(self, bucket_name: str):
        return self.client.check_for_bucket(bucket_name)

    def check_for_object(self, bucket_name: str, object_prefix: str, delimiter: str = '/'):
        return self.client.check_for_prefix(object_prefix, bucket_name=bucket_name, delimiter=delimiter)

    def create_bucket(self, bucket_name: str):
        if not self.client.check_for_bucket(bucket_name):
            self.client.create_bucket(bucket_name)
            return True
        else:
            logging.info(f"Bucket {bucket_name} already exists.")
            return False

    def read_object(self, bucket_name: str, key: str, binary: bool = False):
        if binary:
            file_content = self.client.get_key(key, bucket_name)
            return file_content.get()['Body'].read()
        else:
            return self.client.read_key(key, bucket_name)

    def upload_file(self, file_path: Union[str, Path], bucket_name: str, key: str, replace: bool = True):
        if file_path.is_file():
            self.client.load_file(filename=str(file_path), key=key, bucket_name=bucket_name, replace=replace)
        else:
            raise ValueError(f'{file_path} is not a file.')

    def upload_directory(self, dir_to_upload: Union[str, Path], bucket_name: str, key_prefix: str,
                         replace: bool = True, cleanup: bool = False):
        for file_path in Path(dir_to_upload).glob('**/*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(dir_to_upload)
                file_key = f"{key_prefix}/{relative_path}"
                self.upload_file(file_path, bucket_name, file_key, replace)

        if cleanup:
            shutil.rmtree(dir_to_upload)

    def upload_string(self, string_data: Union[str, Path], bucket_name: str, key: str, replace: bool = True):
        self.client.load_string(string_data, key=key, bucket_name=bucket_name, replace=replace)
