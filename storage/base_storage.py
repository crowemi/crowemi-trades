import os

from pyarrow import fs

from abc import ABCMeta, abstractmethod
import boto3

STORAGE_TYPES = [
    'aws',
]

class BaseStorage(metaclass=ABCMeta):
    def __init__(self, type: str, region: str) -> None:
        if type not in STORAGE_TYPES:
            raise Exception("Invalid storage type.")

        if type == 'aws':
            self.client = boto3.client('s3')
            self.file_system = fs.S3FileSystem(
                access_key=os.getenv('aws_access_key_id'),
                secret_key=os.getenv('aws_secret_access_key')
            )

    @abstractmethod
    def write():
        pass

    @abstractmethod
    def read():
        pass