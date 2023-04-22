import os
import logging
from datetime import datetime

from polars import DataFrame, Series, concat
from pyarrow import Table, Schema

from abc import ABCMeta, abstractmethod

from crowemi_trades.helpers.logging import *

STORAGE_TYPES = [
    "aws",
]


class BaseStorage(metaclass=ABCMeta):
    def __init__(self, type: str) -> None:
        self.LOGGER = create_logger(__name__)
        self.LOGGER.debug("BaseStorage enter.")

        if type not in STORAGE_TYPES:
            raise Exception("Invalid storage type.")
        self.type = type

        self.LOGGER.debug("BaseStorage exit.")

    @abstractmethod
    def get_data(self, start_date: datetime, end_date: datetime) -> DataFrame:
        """Gets historical data from storage."""
        raise NotImplementedError("BaseStorage.get_historical_data: No base implementation for get_data method.")

    @abstractmethod
    def write(
        self,
        bucket: str,
        key: str,
        contents: bytes,
    ) -> bool:
        """Writes contents to a file in cloud storage."""
        raise Exception("BaseStorage.write: No base implementation for write method.")

    @abstractmethod
    def read():
        raise Exception("BaseStorage.read: No base implementation for read method.")

    # FIXME: move to S3 storage class
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

    def _combine_data(
        self,
        primary: DataFrame,
        incoming: DataFrame,
    ) -> DataFrame:
        """Combines raw dataframes into single dataframe."""
        self.LOGGER.debug("Enter _combine_data.")
        try:
            if primary.is_empty():
                primary = incoming
            else:
                if len(primary.columns) != len(incoming.columns):
                    self.LOGGER.warn(
                        f"DataFrame sizes don't match. Primary ({len(primary.columns)}); Incoming ({len(incoming.columns)})."
                    )
                    self.LOGGER.warn("Attempting to reconcile descrepencies.")
                    for col in primary.columns:
                        if not col in incoming.columns:
                            incoming.with_column(
                                Series(name=col, values=None),
                            )
                            self.LOGGER.debug(
                                f"{col} added to incoming. Incoming ({len(incoming.columns)})."
                            )
                    for col in incoming.columns:
                        if not col in primary.columns:
                            primary.with_column(
                                Series(name=col, values=None),
                            )
                            self.LOGGER.debug(
                                f"{col} added to primary. Primary ({len(primary.columns)})."
                            )

                primary = concat([primary, incoming], how="vertical")
        except Exception as e:
            self.LOGGER.error("Unable to concat dataframes")
            self.LOGGER.exception(e)
        self.LOGGER.debug("Exit _combine_data.")
        return primary
