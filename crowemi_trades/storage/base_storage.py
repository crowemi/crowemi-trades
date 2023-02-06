import os
import logging

from polars import DataFrame
from pyarrow import Table, Schema
from pyarrow import fs

from abc import ABCMeta, abstractmethod
import boto3

STORAGE_TYPES = [
    "aws",
]


class BaseStorage(metaclass=ABCMeta):
    def __init__(self, type: str, region: str) -> None:

        self.LOGGER = logging.getLogger("crowemi-trades.storage")

        if type not in STORAGE_TYPES:
            raise Exception("Invalid storage type.")

    @abstractmethod
    def write():
        pass

    @abstractmethod
    def read():
        pass

    def create_data_table(self, df: DataFrame) -> tuple[Table, Schema]:
        """Creates a pyarrow Table object from a polars dataframe."""
        try:
            data = Table.from_pandas(df.to_pandas())
            schema = Table.from_pandas(df.to_pandas()).schema
            return data, schema
        except Exception as e:
            self.LOGGER.error("Failed to create parquet dataframe.")
            self.LOGGER.exception(e)
            raise e
