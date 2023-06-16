import os
import logging
from datetime import datetime
from abc import ABCMeta, abstractmethod

from polars import DataFrame, Series, concat
import polars as pl

from crowemi_trades.helpers.logging import *


STORAGE_TYPES = [
    "aws",
    "mongodb",
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
    def read(
        self,
        ticker: str,
        interval: str,
        timespan: str,
        start_date: datetime = None,
        end_date: datetime = None,
        results_only: bool = False,
    ) -> DataFrame:
        raise NotImplementedError(
            "BaseStorage.read: No base implementation for read method."
        )

    @abstractmethod
    def write(
        self,
        records: dict = None,
        **kwargs,
    ) -> bool:
        """Writes contents to a file in cloud storage."""
        raise NotImplementedError(
            "BaseStorage.write: No base implementation for write method."
        )

    @staticmethod
    def combine_data(
        primary: DataFrame,
        incoming: DataFrame,
    ) -> DataFrame:
        """Combines raw dataframes into single dataframe."""
        try:
            if primary.is_empty():
                primary = incoming
            else:
                if len(primary.columns) != len(incoming.columns):
                    # TODO: DRY!
                    for col in primary.columns:
                        if not col in incoming.columns:
                            dtype = primary.schema[col]
                            # pl.Series('empty lists', [[]], dtype=pl.List),
                            incoming = incoming.with_columns(
                                pl.lit(None).alias(col).cast(dtype)
                            )
                    for col in incoming.columns:
                        if not col in primary.columns:
                            dtype = incoming.schema[col]
                            primary = primary.with_columns(
                                pl.lit(None).alias(col).cast(dtype)
                            )

                primary = concat(
                    [
                        primary.select(sorted(primary.columns)),
                        incoming.select(sorted(incoming.columns)),
                    ],
                    how="vertical",
                )
        except Exception as e:
            print("Unable to concat dataframes")
            raise e
        return primary
